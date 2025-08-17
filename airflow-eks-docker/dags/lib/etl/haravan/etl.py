from __future__ import annotations
from typing import Dict, Optional, Tuple, List
from datetime import datetime, timedelta
from urllib.parse import urlparse
import io, json, gzip, re, requests, pandas as pd, pyarrow as pa, pyarrow.parquet as pq, pytz
from io import BytesIO
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

from ..base import ApiETL
from ..factory import register_pipeline
from ..dw import DatawarehouseConnection


VIETNAM_TZ = pytz.timezone("Asia/Ho_Chi_Minh")


@register_pipeline("haravan")
class HaravanETL(ApiETL):
    """
    Implementation of ApiETL for Haravan API.
    """

    def __init__(
        self, api_urls, headers, s3_bronze_bucket, s3_silver_bucket, aws_conn_id: str = "aws_haravan", redshift_connection='', aws_access_key_id='', aws_secret_access_key=''
    ):
        self.api_urls = api_urls
        self.headers = headers
        self.s3_bronze_bucket = s3_bronze_bucket
        self.s3_silver_bucket = s3_silver_bucket
        self.aws_conn_id = aws_conn_id
        self.session = AwsBaseHook(aws_conn_id=aws_conn_id).get_session()
        self.s3_client = self.session.client("s3")
        self.redshift_connection = redshift_connection
        self.vietnam_time = datetime.now(VIETNAM_TZ)
        self.timestamp = self.vietnam_time.strftime("%Y%m%d_%H%M%S")
        self.s3_file_paths = {}
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        

    def extract(self, api_url: str, params: dict, endpoint_name: str) -> str:
        """
        Extract data from Haravan API and save directly to S3 in JSON format.
        """
        page = 1
        all_data = []
        params.update({"page": page, "limit": 50, "sort_by": "updated_at"})
 
        while True:
            response = requests.get(api_url, headers=self.headers, params=params)
            if response.status_code == 200:
                data = response.json()
                items = data.get(list(data.keys())[0], [])
                if not items:
                    break
                all_data.extend(items)
                page += 1
                params["page"] = page
            else:
                print(f"Failed to fetch data for {endpoint_name}: {response.status_code}")
                break
 
        # Upload JSON data to S3
        current_time = datetime.now(VIETNAM_TZ)
        year = current_time.strftime("%Y")
        month = current_time.strftime("%m")
        day = current_time.strftime("%d")
 
 
        s3_path = f"Haravan/OpenAPI/{endpoint_name}/{year}/{month}/{day}/{endpoint_name}_{self.timestamp}.json"
        json_data = json.dumps(all_data, ensure_ascii=False, indent=4)
        self.s3_client.put_object(Bucket=self.s3_bronze_bucket, Key=s3_path, Body=json_data)
        print(f"Data uploaded to S3 for {endpoint_name}: {s3_path}")
        return s3_path

    def transform(self, dataframes: dict) -> dict:
        """
        Transform the extracted data for further processing.

        Args:
            dataframes (dict): Dictionary of DataFrames loaded from JSON files.

        Returns:
            dict: Transformed DataFrames.
        """
        # Deduplicate DataFrames
        for name in dataframes.keys():
            dataframes[name] = dataframes[name].drop_duplicates(subset=["id"], keep="last")
        
        # Transform 'orders' to create 'line_items_order' and 'properties_line_items_order'
        if "orders" in dataframes.keys():
            # Expand line_items to create 'line_items_order'
            line_items_expanded = []
            for _, row in dataframes["orders"].iterrows():
                line_items = row.get("line_items", [])
                order_id = row["id"]
                updated_at = row["updated_at"]
                if line_items:
                    for item in line_items:
                        item["order_id"] = order_id
                        item["updated_at"] = updated_at
                        line_items_expanded.append(item)
            
            dataframes["line_items_order"] = pd.DataFrame(line_items_expanded)
            dataframes["line_items_order"]["image_src"] = dataframes["line_items_order"]["image"].apply(
                lambda x: x["src"] if isinstance(x, dict) and "src" in x else None
            )

            # Expand properties to create 'properties_line_items_order'
            properties_expanded = []
            for _, row in dataframes["line_items_order"].iterrows():
                properties = row.get("properties", [])
                line_item_id = row["id"]
                updated_at = row["updated_at"]
                if properties:
                    for item in properties:
                        item["line_item_id"] = line_item_id
                        item["updated_at"] = updated_at
                        properties_expanded.append(item)
            
            dataframes["properties_line_items_order"] = pd.DataFrame(properties_expanded)

        # Transform 'products' to create 'variants_product'
        if "products" in dataframes.keys():
            variants_product_expanded = []
            for _, row in dataframes["products"].iterrows():
                variants = row.get("variants", [])
                product_id = row["id"]
                updated_at = row["updated_at"]
                if variants:
                    for variant in variants:
                        variant["product_id"] = product_id
                        variant["updated_at"] = updated_at
                        variants_product_expanded.append(variant)
            
            dataframes["variants_product"] = pd.DataFrame(variants_product_expanded)

        # Align schemas and convert datetime columns
        s3_silver_bucket_name = self.s3_silver_bucket
        for name, df in dataframes.items():
            s3_template_format = f"ComC/E-selling/{name}/fullData_to_20241029/cleaned_{name}_20241029.parquet"
            schema = self._get_parquet_schema_from_s3(s3_silver_bucket_name, s3_template_format, self.session)
            df = self._align_df_to_schema(df, schema)
            dataframes[name] = self._convert_to_vietnamese_datetime(df)

        print("Transformation complete.")
        return dataframes
    
    def push_transformed_to_s3(self, dataframes: dict) -> dict:
        """
        Push transformed DataFrames to the Silver S3 bucket.

        Args:
            dataframes (dict): Dictionary of transformed DataFrames.

        Returns:
            dict: Dictionary of S3 paths where the DataFrames are saved.
        """
        s3_silver_paths = {}
        for name, df in dataframes.items():
            current_time = datetime.now(VIETNAM_TZ)
            year = current_time.strftime("%Y")
            month = current_time.strftime("%m")
            day = current_time.strftime("%d")
            timestamp = current_time.strftime("%Y%m%d_%H%M%S")
            
            s3_incremental_file_name = f"ComC/E-selling/{name}/{year}/{month}/{day}/cleaned_{name}_{timestamp}.parquet"
            s3_path = f"s3://{self.s3_silver_bucket}/{s3_incremental_file_name}"
            self._push_dataframe_to_s3(df, self.s3_silver_bucket, s3_incremental_file_name, self.session)
            s3_silver_paths[name] = s3_path
        
        print("Transformed DataFrames uploaded to Silver S3 bucket.")
        return s3_silver_paths
    
    def load(self, s3_silver_paths):
        """
        Load transformed data into Redshift using the Redshift connection.
        """
        with self.redshift_connection as conn:
            for name, path in s3_silver_paths.items():
                print(f"Loading table: {name}")
                
                if name in ["orders", "products", "customers", "line_items_order", "properties_line_items_order", "variants_product"]:
                    # Configure the CDC merge parameters
                    config = {
                        "table_name": f"comc_eselling_stg__haravanapi__{name}",
                        "s3_incremental_file_path": path,
                        "schema_name": "comc_eselling",
                        "key_column": "id",
                        "join_column": "id",
                        "handle_deletes": False,
                    }

                    if name == "line_items_order":
                        config["join_column"] = "order_id"
                        config["handle_deletes"] =True
                    elif name == "properties_line_items_order":
                        config["key_column"] = "line_item_id"
                        config["join_column"] = "line_item_id"
                        config["handle_deletes"] =True
                    elif name == "variants_product":
                        config["join_column"] = "product_id"
                        config["handle_deletes"] =True

                    # Perform CDC merge
                    self._merge_new_data_to_redshift(
                        redshift_conn=conn,
                        s3_incremental_file_path=config["s3_incremental_file_path"],
                        table_name=config["table_name"],
                        schema_name=config["schema_name"],
                        key_column=config["key_column"],
                        join_column=config["join_column"],
                        handle_deletes=config["handle_deletes"]
                    )
                else:
                    # Perform full load for other tables
                    self._copy_data_to_redshift(
                        redshift_conn=conn,
                        s3_path=path,
                        schema_name="comc_eselling",
                        table_name=f"comc_eselling_stg__haravanapi__{name}"
                    )
                print(f"Successfully loaded table {name}.")

    #################################################################
    ########### support function:
    def read_json_files_from_s3(self,s3_file_paths, s3_bucket, session):
        # session = boto3.Session(profile_name=profile_name)
        print(s3_bucket)
        print(s3_file_paths)
        s3_client = session.client('s3')
        dataframes = {}

        for name, file_path in s3_file_paths.items():
            # Fetch the JSON file from S3
            obj = s3_client.get_object(Bucket=s3_bucket, Key=file_path)
            json_content = obj['Body'].read().decode('utf-8')
            
            # Load JSON content
            data = json.loads(json_content)
            
            # If the JSON data is a list of records, load it into a DataFrame
            if isinstance(data, list):
                df = pd.json_normalize(data)
            else:
                # If the JSON has a nested structure with a main key
                main_key = list(data.keys())[0]
                df = pd.json_normalize(data[main_key])

            dataframes[name] = df
            print(f"Loaded DataFrame for {name} with {len(df)} records.")

        return dataframes

    def _is_datetime_column(self,series):
        non_na_series = series.dropna()
        
        if len(non_na_series) == 0:  # Check if the series is empty after dropping NaNs
            return False
        
        # Regex pattern to match typical date formats (e.g. "2022-12-31", "12/31/2022", etc.)
        date_pattern = re.compile(r'^\d{1,4}[-/]\d{1,2}[-/]\d{1,4}')
        
        # Check if most non-NaN values follow a date-like pattern
        match_count = non_na_series.apply(lambda x: bool(date_pattern.match(str(x)))).sum()
        
        if match_count / len(non_na_series) < 0.7:  # Ensure at least 70% of the values are date-like
            return False
        
        try:
            # Try parsing the series as a datetime, allowing coercion of invalid values
            converted = pd.to_datetime(non_na_series, errors='coerce')
            
            # If at least 70% of the non-null values are successfully converted, treat it as datetime
            if converted.notnull().sum() / len(non_na_series) >= 0.7:
                return True
            else:
                return False
        except Exception:
            return False


    def _parse_mixed_datetime_formats(self,series):
        """
        This function handles datetime columns with mixed formats (e.g. with milliseconds and without).
        """
        # Ensure the series is in string format and replace "None" or "NaN" placeholders
        series = series.astype(str).replace(["None", "nan", "NaT", "NaN"], pd.NA)
        
        # Create an empty series initialized with NaT
        result = pd.Series(pd.NaT, index=series.index, dtype='datetime64[ns, UTC]')

        # Handle the format '%Y-%m-%dT%H:%M:%S.%fZ'
        mask_milliseconds = series.str.contains(r'\.\d{1,6}Z$', na=False)
        parsed_milliseconds = pd.to_datetime(series[mask_milliseconds], format='%Y-%m-%dT%H:%M:%S.%fZ', errors='coerce', utc=True)

        # Handle the format '%Y-%m-%dT%H:%M:%SZ'
        mask_seconds = series.str.contains(r'Z$', na=False) & ~mask_milliseconds
        parsed_seconds = pd.to_datetime(series[mask_seconds], format='%Y-%m-%dT%H:%M:%SZ', errors='coerce', utc=True)

        # Assign parsed results back to their respective positions
        result[mask_milliseconds] = parsed_milliseconds
        result[mask_seconds] = parsed_seconds

        return result

    def _transform_df_before_push(self,df):
        
        # Convert all columns containing JSON-like data to strings
        def convert_json_columns(x):
            if isinstance(x, (list, dict)):
                return json.dumps(x, ensure_ascii=False) 
            return x

        # Apply conversion to all object-type columns
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].apply(convert_json_columns)
            
        datetime_columns = []
        # Iterate through the columns and convert them to datetime if possible
        for col in df.columns:
            if col.lower().endswith('id'):
                # Convert float to integer first, then to string
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64').astype(str)
                # df[col] = df[col].astype(str)
                print(f"Column '{col}' converted to string format.")
            elif df[col].isna().all():
                df[col] = df[col].astype(str)
                print(f"Column '{col}' converted to string format.")
            elif df[col].dtype == 'object':  # Only check object type columns
                if self._is_datetime_column(df[col]):
                    datetime_columns.append(col)
                    # Convert to datetime first
                    df[col] = self._parse_mixed_datetime_formats(df[col])
                    # Round the datetime to milliseconds
                    df[col] = df[col].dt.tz_convert('UTC').dt.tz_localize(None).astype('datetime64[ms]')
                    print(f"Column '{col}' converted to datetime64[ms] format.")
            elif df[col].dtype == 'datetime64[ns, UTC]':
                df[col] = df[col].dt.tz_convert('UTC').dt.tz_localize(None).astype('datetime64[ms]')

        # return "Success transform datetime, __id, Nan, json column into string column to work with dataframe to push into S3 or Redshift..."
        return datetime_columns

    def _get_parquet_schema_from_s3(self,bucket_name, s3_file_name, session):
        # session = boto3.Session(profile_name=profile_name)
        s3 = session.client('s3')

        # Download the existing parquet file from S3
        parquet_buffer = BytesIO()
        s3.download_fileobj(Bucket=bucket_name, Key=s3_file_name, Fileobj=parquet_buffer)
        
        # Load the Parquet file schema
        parquet_buffer.seek(0)
        table = pq.read_table(parquet_buffer)
        return table.schema

    def _align_df_to_schema(self,df, schema):
        for field in schema:
            col_name = field.name
            
            # Check if the column exists in the dataframe, if not, create it with the correct dtype
            if col_name not in df.columns:
                df[col_name] = None  # Add missing columns

            # Map PyArrow types to pandas types
            if pa.types.is_integer(field.type):
                # Convert to standard `int64` instead of nullable `Int64Dtype`
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype('int64')  # Non-nullable integer columns
            elif pa.types.is_floating(field.type):
                # Coerce invalid strings to NaN, then convert to float
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype(float)  # Float columns
            elif pa.types.is_boolean(field.type):
                # Convert to proper boolean dtype (to support NaNs and null values)
                df[col_name] = df[col_name].astype(pd.BooleanDtype())  # Boolean columns with NaN support
            elif pa.types.is_timestamp(field.type):
                # Handle datetime columns
                df[col_name] = pd.to_datetime(df[col_name], errors='coerce')  # Convert to datetime

                # Check if the datetime column is timezone-naive and localize it to UTC
                if df[col_name].dt.tz is None:
                    df[col_name] = df[col_name].dt.tz_localize('UTC')  # Localize to UTC

                # Now convert to UTC and round to milliseconds
                df[col_name] = df[col_name].dt.tz_convert('UTC').dt.tz_localize(None).astype('datetime64[ms]')
            else:
                if col_name.lower().endswith('id'):
                # Convert float to integer first, then to string
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype('Int64').astype(str)
                else:
                    df[col_name] = df[col_name].astype(str)  # Default to string for any other type

        return df[schema.names]  # Reorder columns to match the schema


    def _push_dataframe_to_s3(self,df, bucket_name, s3_file_name, session):
        # Initialize a session using Amazon S3 with a specific profile
        # session = boto3.Session(profile_name=profile_name)
        s3 = session.client("s3")

        # Transform the data before pushing
        # transform_df_before_push(df)
        
        # Store the DataFrame to Parquet in memory
        parquet_buffer = BytesIO()

        # Use pyarrow to handle Parquet conversion, drop the index by setting `preserve_index=False`
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, parquet_buffer)

        # Upload the Parquet file to S3
        s3.put_object(Bucket=bucket_name, Key=s3_file_name, Body=parquet_buffer.getvalue())
        print(f"Dataframe uploaded to S3 bucket '{bucket_name}' as '{s3_file_name}'")

    def _convert_to_vietnamese_datetime(self,df):
        global VIETNAM_TZ

        # Find all datetime columns with 'datetime64' dtype (ignores specific precision)
        datetime_columns = df.select_dtypes(include=['datetime64']).columns

        # Convert each datetime column from UTC to Vietnam timezone
        for col in datetime_columns:
            # If the column is timezone-naive, localize it to UTC first
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize('UTC')

            # Convert to Vietnam timezone and set precision to milliseconds
            df[col] = df[col].dt.tz_convert(VIETNAM_TZ).dt.tz_localize(None).astype('datetime64[ms]')
            print(f"Column '{col}' converted to Vietnam timezone with 'datetime64[ms]' precision.")

        return df   
    
    # Build dynamic merge query
    def _quote_column(self, col):
        """Quotes column names with special characters (like periods)"""
        if '.' in col or ' ' in col:
            return f'"{col}"'
        return col
    
    def _merge_new_data_to_redshift(
        self,
        redshift_conn: DatawarehouseConnection,
        s3_incremental_file_path: str,
        table_name: str,
        schema_name: str,
        key_column: str,
        join_column: str,
        handle_deletes: bool = False,
    ):
        """
        Perform CDC merge with new data into Redshift using the provided DatawarehouseConnection.
        """
        with redshift_conn.managed_cursor() as cursor:
            # Fetch column names
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}';")
            columns = [row[0] for row in cursor.fetchall()]

            # Create a temporary table
            suffix_table = datetime.now().strftime('%Y%m%d_%H%M%S')
            temp_table = f'temp_new_{table_name}_{suffix_table}'
            cursor.execute(f"""
                CREATE TEMP TABLE {temp_table} (LIKE "{schema_name}"."{table_name}");
            """)

            # Copy data from S3 into the temporary table
            cursor.execute(f"""
                COPY {temp_table}
                FROM '{s3_incremental_file_path}'
                CREDENTIALS 'aws_access_key_id={self.aws_access_key_id};aws_secret_access_key={self.aws_secret_access_key}'
                FORMAT AS PARQUET;
            """)
            

            # Update columns excluding the key column(s)
            update_columns = ", ".join([f'{self._quote_column(col)} = temp.{self._quote_column(col)}' for col in columns if col != key_column and col != 'updated_at'])

            # Handle deletes if necessary
            if handle_deletes:
                delete_query = f"""
                    DELETE FROM "{schema_name}"."{table_name}"
                    USING (
                        SELECT {join_column}, MAX(updated_at) AS updated_at
                        FROM {temp_table}
                        GROUP BY {join_column}
                    ) AS temp
                    WHERE "{schema_name}"."{table_name}".{join_column} = temp.{join_column}
                    AND temp.updated_at > "{schema_name}"."{table_name}".updated_at;
                """
                cursor.execute(delete_query)

            # Merge query: update existing rows and insert new ones
            # Update and insert logic
            if update_columns:
                merge_query = f"""
                    BEGIN;

                    -- Update existing records in the main table
                    UPDATE "{schema_name}"."{table_name}"
                    SET {update_columns},
                        "updated_at" = temp."updated_at"
                    FROM {temp_table} temp
                    WHERE "{schema_name}"."{table_name}".{key_column} = temp.{key_column}
                    AND temp.updated_at > "{schema_name}"."{table_name}".updated_at;

                    -- Insert new records into the main table
                    INSERT INTO "{schema_name}"."{table_name}" ({','.join([self._quote_column(col) for col in columns])})
                    SELECT {','.join([f'temp.{self._quote_column(col)}' for col in columns])}
                    FROM {temp_table} temp
                    LEFT JOIN "{schema_name}"."{table_name}" main
                    ON temp.{key_column} = main.{key_column}
                    WHERE main.{key_column} IS NULL;

                    COMMIT;
                """
                cursor.execute(merge_query)

        print(f"""Merge completed for "{schema_name}"."{table_name}".""")  
    
    def _copy_data_to_redshift(
        self,
        redshift_conn: DatawarehouseConnection,
        s3_path: str,
        schema_name: str,
        table_name: str
    ):
        with redshift_conn.managed_cursor() as cursor:
            cursor.execute(f"""TRUNCATE TABLE "{schema_name}"."{table_name}";""")

            copy_query = f"""
            COPY "{schema_name}"."{table_name}"
            FROM '{s3_path}'
            CREDENTIALS 'aws_access_key_id={self.aws_access_key_id};aws_secret_access_key={self.aws_secret_access_key}'
            FORMAT AS PARQUET;
            """
            cursor.execute(copy_query)

        print(f"""Data copied to "{schema_name}"."{table_name}".""")   
        
         
    def _map_parquet_to_redshift(self, arrow_schema: pa.Schema, super_enabled: bool = False) -> str:
        """
        Map a pyarrow.Schema to a Redshift column definition list.
        """

        def to_redshift_type(field: pa.Field) -> str:
            t = field.type

            # integers
            if pa.types.is_int64(t):
                return "BIGINT"
            if pa.types.is_integer(t):  # covers int8/16/32/uint*
                return "INTEGER"

            # floats
            if pa.types.is_float64(t):
                return "FLOAT8"  # alias for DOUBLE PRECISION
            if pa.types.is_float32(t) or pa.types.is_float16(t):
                return "REAL"

            # booleans, dates, timestamps
            if pa.types.is_boolean(t):
                return "BOOLEAN"
            if pa.types.is_date(t):
                return "DATE"
            if pa.types.is_timestamp(t):
                # Redshift stores in UTC; use TIMESTAMPTZ if timezone present
                return "TIMESTAMPTZ" if getattr(t, "tz", None) else "TIMESTAMP"

            # decimals/binary/strings
            if pa.types.is_decimal(t):
                return f"DECIMAL({t.precision},{t.scale})"
            if pa.types.is_binary(t) or pa.types.is_large_binary(t):
                return "VARBYTE"
            if pa.types.is_string(t) or pa.types.is_large_string(t):
                return "VARCHAR(65535)"

            # nested/complex
            if (
                pa.types.is_list(t) or pa.types.is_large_list(t)
                or pa.types.is_struct(t) or pa.types.is_map(t)
            ):
                return "SUPER" if super_enabled else "VARCHAR(65535)"

            # default fallback
            return "VARCHAR(65535)"

        cols = [f'"{f.name}" {to_redshift_type(f)}' for f in arrow_schema]
        return ", ".join(cols)


    def _create_redshift_table_sql(
        self,
        schema_name: str,
        table_name: str,
        parquet_s3_path: str,
        aws_conn_id: str = "aws_haravan",
        super_enabled: bool = False,
    ) -> str:
        """
        Read a Parquet file's schema from S3 using the Airflow AWS connection,
        map to Redshift, and return a CREATE TABLE statement.
        """
        from urllib.parse import urlparse
        # Build a session from the Airflow connection (supports IRSA, static keys, AssumeRole)
        hook = AwsBaseHook(aws_conn_id=aws_conn_id)
        session = hook.get_session()
        creds = session.get_credentials().get_frozen_credentials()

        # Native pyarrow S3 filesystem (no s3fs)
        fs = pa.fs.S3FileSystem(
            access_key=creds.access_key,
            secret_key=creds.secret_key,
            session_token=creds.token,
            region=session.region_name,
        )

        # Parse s3://bucket/key into the path S3FileSystem expects
        p = urlparse(parquet_s3_path)  # e.g., s3://my-bucket/path/to/file.parquet
        s3_path_for_fs = f"{p.netloc}/{p.path.lstrip('/')}"

        # Load Arrow schema from Parquet
        with fs.open_input_file(s3_path_for_fs) as f:
            pq_file = pq.ParquetFile(f)
            arrow_schema = pq_file.schema_arrow  # prefer Arrow schema for robust typing

        redshift_cols = self._map_parquet_to_redshift(arrow_schema, super_enabled=super_enabled)

        return f'''CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
    {redshift_cols}
    );'''
    
    def _create_redshift_table(
        self,redshift_conn: DatawarehouseConnection, schema_name: str, table_name: str, create_sql: str
    ):
        with redshift_conn.managed_cursor() as cursor:
            cursor.execute(create_sql)

        print(f"Table {schema_name}.{table_name} created.")
        
    def _quote_column(self,col):
        """Quotes column names with special characters (like periods)"""
        if '.' in col or ' ' in col:
            return f'"{col}"'
        return col
    #######################################################

