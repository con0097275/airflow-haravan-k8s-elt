from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.hooks.base import BaseHook


# # K8s resources per task (CPU/RAM)
from kubernetes.client import models as k8s
from typing import Dict


# ----- Airflow DAG basics -----
DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SCHEDULE = Variable.get("HARAVAN_SCHEDULE", default_var="@hourly")
START_DATE = datetime(2025, 1, 1)

# ----- Per-step K8s resource configs (tune freely) -----
def pod_override_for(cpu_req: str, mem_req: str, cpu_lim: str, mem_lim: str, image: str | None = None) -> k8s.V1Pod:
    """
    Build a V1Pod pod_override for KubernetesExecutor.
    The container name must be 'base' to match Airflow's template.
    """
    container = k8s.V1Container(
        name="base",
        resources=k8s.V1ResourceRequirements(
            requests={"cpu": cpu_req, "memory": mem_req},
            limits={"cpu": cpu_lim, "memory": mem_lim},
        ),
    )
    if image:
        container.image = image

    return k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[container],
            restart_policy="Never",
        )
    )

EXTRACT_POD = pod_override_for(cpu_req="500m", mem_req="1Gi", cpu_lim="1",   mem_lim="2Gi")
TRANSFORM_POD = pod_override_for(cpu_req="1000m", mem_req="1Gi", cpu_lim="1.5",   mem_lim="4Gi")
LOAD_POD = pod_override_for(cpu_req="500m", mem_req="1Gi", cpu_lim="1",      mem_lim="2Gi")

# (Optional) if you need a different image for any step, pass image="your-ecr/image:tag" to pod_override_for


# ----- Helper: create ETL instance from Airflow Connections/Variables -----
def build_etl(pipeline_name: str):
    from lib.etl import ETLFactory, DatawarehouseConnection  # ← from lib package
    # API + buckets
    api_urls = Variable.get(
        "HARAVAN_API_URLS",
        default_var='{"orders":"https://apis.haravan.com/com/orders.json",'
                    '"discounts":"https://apis.haravan.com/com/discounts.json",'
                    '"promotions":"https://apis.haravan.com/com/promotions.json",'
                    '"customers":"https://apis.haravan.com/com/customers.json"}',
        deserialize_json=True,
    )
    s3_bronze_bucket = Variable.get("HARAVAN_S3_BRONZE_BUCKET", default_var="vannk-dev-bronze")
    s3_silver_bucket = Variable.get("HARAVAN_S3_SILVER_BUCKET", default_var="vannk-dev-silver")
    haravan_token = Variable.get("HARAVAN_BEARER_TOKEN")  # or use Secrets Manager backend
    REDSHIFT_CONN_ID  = Variable.get("HARAVAN_REDSHIFT_CONN_ID",  default_var="haravan_redshift")
    
    # Use Airflow Connections (recommended) rather than hard-coding
    # Redshift conn id configured as a Postgres/Redshift connection in Airflow UI
    rs = BaseHook.get_connection(REDSHIFT_CONN_ID)
    # sslmode = (rs.extra_dejson or {}).get("sslmode", "require")
    redshift_conn = DatawarehouseConnection(
        host=rs.host,
        port=rs.port or 5439,
        database=rs.schema,
        user=rs.login,
        password=rs.password,
        # if class supports sslmode, pass it; or read it inside .connect()
        # sslmode=sslmode,
    )

    # Optionally pull AWS creds from an AWS connection (only if you don’t use IRSA)
    # If you run on EKS with IRSA, you can omit and let boto3 pick up the role

    AWS_CONN_ID = Variable.get("HARAVAN_AWS_CONN_ID", default_var="aws_haravan")
    try:
        aws = BaseHook.get_connection(AWS_CONN_ID)
        aws_access_key_id = aws.login
        aws_secret_access_key = aws.password
    except Exception:
        aws_access_key_id = None
        aws_secret_access_key = None

    headers = {"Authorization": f"Bearer {haravan_token}"}
    

    return ETLFactory.create(
        pipeline_name,
        api_urls=api_urls,
        headers=headers,
        s3_bronze_bucket=s3_bronze_bucket,
        s3_silver_bucket=s3_silver_bucket,
        aws_conn_id = "aws_haravan",
        redshift_connection=redshift_conn,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )


# ----- DAG definition -----
with DAG(
    dag_id="haravan_etl_k8s",
    default_args=DEFAULT_ARGS,
    start_date=START_DATE,
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["haravan", "kubernetes", "factory"],
) as dag:
    
    @task(executor_config={"pod_override": EXTRACT_POD})
    def extract_to_bronze() -> Dict[str, str]:
        """
        extract_to_bronze: calls your class's extract per endpoint and writes to S3 Bronze.
        Returns dict: {endpoint: s3_key_or_prefix}
        """
        etl = build_etl('haravan')

        # Build time window per your original logic
        bronze_paths: Dict[str, str] = {}
        from datetime import timezone
        import pytz

        VIETNAM_TZ = pytz.timezone("Asia/Ho_Chi_Minh")
        numdays = float(Variable.get("HARAVAN_WINDOW_DAYS", default_var="0.1"))  # ~2.4 hours by default
        vietnam_time = datetime.now(VIETNAM_TZ)
        updated_at_max_time_vn = vietnam_time
        updated_at_min_time_vn = vietnam_time - timedelta(days=numdays)

        for name, api_url in etl.api_urls.items():
            if name in ["orders", "products"]:
                updated_at_min = updated_at_min_time_vn.strftime("%Y-%m-%dT%H:%M:%S")
                updated_at_max = updated_at_max_time_vn.strftime("%Y-%m-%dT%H:%M:%S")
            elif name == "customers":
                updated_at_min = updated_at_min_time_vn.isoformat(timespec="seconds")
                updated_at_max = (updated_at_max_time_vn + timedelta(hours=7)).isoformat(timespec="seconds")
            else:
                updated_at_min = None
                updated_at_max = None

            params = {"updated_at_min": updated_at_min, "updated_at_max": updated_at_max}
            s3_path = etl.extract(api_url, params, endpoint_name=name)
            bronze_paths[name] = s3_path

        return bronze_paths

    @task(executor_config={"pod_override": TRANSFORM_POD})
    def bronze_to_silver(bronze_paths: Dict[str, str]) -> Dict[str, str]:
        """
        bronze_to_silver: reads Bronze from S3, transforms, writes to S3 Silver.
        Returns dict: {endpoint: silver_s3_key_or_prefix}
        """
        etl = build_etl('haravan')

        # Read raw (Bronze)
        dfs = etl.read_json_files_from_s3(
            s3_file_paths=bronze_paths,
            s3_bucket=etl.s3_bronze_bucket,
            session=etl.session,
        )

        # Transform (your own method) -> dataframe
        transformed = etl.transform(dfs)

        # Write to Silver -> parquet format, return paths
        silver_paths = etl.push_transformed_to_s3(transformed)
        return silver_paths

    @task(executor_config={"pod_override": LOAD_POD})
    def merge_silver_to_redshift(silver_paths: Dict[str, str]) -> str:
        """
        merge_silver_to_redshift: reads Silver from S3 and loads to Redshift (via your .load()).
        """
        etl = build_etl('haravan')
        print(silver_paths)
        etl.load(silver_paths)
        return "Merged"

    # Orchestration
    merge_silver_to_redshift(bronze_to_silver(extract_to_bronze()))

