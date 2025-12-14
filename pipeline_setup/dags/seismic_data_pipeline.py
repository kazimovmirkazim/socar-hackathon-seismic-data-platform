from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'seismic_data_etl',
    default_args=default_args,
    description='ETL pipeline for Seismic Data Vault and Analytics',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['seismic', 'etl'],
) as dag:

    # Task 1: Ingest Data into Data Vault
    # The docker-compose mounts the project root to /opt/airflow/project_root
    # We execute the script from that directory so relative paths work.
    ingest_data_vault = BashOperator(
        task_id='ingest_data_vault',
        bash_command='cd /opt/airflow/project_root && python data_vault_builder.py',
    )

    # Task 2: Transform Data into Analytics Layer
    transform_analytics = BashOperator(
        task_id='transform_analytics',
        bash_command='cd /opt/airflow/project_root && python analytics_etl.py',
    )

    ingest_data_vault >> transform_analytics
