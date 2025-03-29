from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator
)
from airflow.operators.dummy_operator import DummyOperator

# Constants
PROJECT_ID = 'ford-3040efdcfdf64026be237121'
REGION = 'us-central1'
CLUSTER_NAME = 'sumanth-cluster'
BUCKET_NAME = 'us-central1-sumanth1-750c33f0-bucket'
INPUT_GCS_PATH = f'gs://{BUCKET_NAME}/dags/input_data.csv'
PYSPARK_URI = f'gs://{BUCKET_NAME}/dags/your_spark_script.py'  # Path to your PySpark script
BQ_DATASET = 'Company'
BQ_TABLE = 'employees'
BQ_FULL_TABLE_ID = f'{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}'

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    'gcs_to_bigquery_dataproc',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    start_task = DummyOperator(task_id='start')

    # Create Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config={
            'gce_cluster_config': {
        'network_uri': 'projects/PROJECT_ID/global/networks/sandbox-vpc'
    },
            'master_config': {
                'num_instances': 1,
                'machine_type_uri': 'e2-micro',
            },
            'worker_config': {
                'num_instances': 2,
                'machine_type_uri': 'e2-micro',
            },
        },
    )

    # Submit PySpark job to Dataproc
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job',
        job={
            'reference': {'project_id': PROJECT_ID},
            'placement': {'cluster_name': CLUSTER_NAME},
            'pyspark_job': {'main_python_file_uri': PYSPARK_URI},
        },
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Check BigQuery table row count
    check_bq_count = BigQueryCheckOperator(
        task_id='check_bq_row_count',
        sql=f'SELECT COUNT(*) FROM `{BQ_FULL_TABLE_ID}`',
        use_legacy_sql=False,
    )

    # Delete Dataproc cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule='all_done',  # Ensure cluster is deleted even if previous tasks fail
    )

    end_task = DummyOperator(task_id='end')

    # Task dependencies
    start_task >> create_cluster >> submit_pyspark_job >> check_bq_count >> delete_cluster >> end_task
