FROM apache/airflow:2.5.1

## airflow version 2.7.0 not work


# LABEL version="1.0.0"
# vanlove



RUN pip install --user pytest

COPY dags/ ${AIRFLOW_HOME}/dags
COPY unittests.cfg ${AIRFLOW_HOME}/unittests.cfg
COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY unittests/ ${AIRFLOW_HOME}/unittests
COPY integrationtests ${AIRFLOW_HOME}/integrationtests







