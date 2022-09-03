import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

from datetime import datetime


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BASE_URL = "https://d37ci6vzurychx.cloudfront.net"

def format_to_parquet(src_file, dest_file):
    if not src_file.endswith('.csv'):
        if src_file.endswith('.parquet'):
            logging.info("Downloaded file is already in parquet format.")
            return
        else:
            logging.error("Unsupported format: downloaded file is not in parquet or CSV format.")
            return
    table = pv.read_csv(src_file)
    pq.write_table(table, dest_file)


def upload_to_gcs(bucket, object_name, local_file):
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "catchup": True,
    "depends_on_past": False,
    "max_active_runs": 3,
    "retries": 1,
}

def download_parquetize_upload_dag(
    dag,
    url_template,
    local_download_path_template,
    gcs_path_template,
    local_parquet_path_template=None
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {url_template} > {local_download_path_template}"
        )
        
        parquetize_csv_task = PythonOperator(
                task_id="parquetize_csv_task",
                python_callable=format_to_parquet,
                op_kwargs={
                    "src_file": f"{local_download_path_template}",
                    "dest_file": f"{local_parquet_path_template}"
                },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"{gcs_path_template}",
                "local_file": f"{local_parquet_path_template if local_parquet_path_template else local_download_path_template}",
            },
        )

        cleanup_local_dataset_download = BashOperator(
            task_id="cleanup_local_dataset_download_task",
            bash_command=f"rm -f {local_download_path_template} {local_parquet_path_template}"
        )

        download_dataset_task >> parquetize_csv_task >> local_to_gcs_task >> cleanup_local_dataset_download


# instantiate DAG for yellow taxi trip data
YELLOW_TAXI_URL_TEMPLATE = BASE_URL + '/trip-data/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
YELLOW_TAXI_OUTPUTFILE_TEMPLATE = AIRFLOW_HOME + '/output_yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_GCS_DESTINATION_TEMPLATE = 'raw/yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

yellow_taxi_ingestion_dag = DAG(
    dag_id="yellow_taxi_ingestion",
    schedule_interval="@monthly",
    end_date=datetime(2021, 12, 31),
    default_args=default_args,
    tags=['dtc-de']
)

download_parquetize_upload_dag(
    yellow_taxi_ingestion_dag,
    YELLOW_TAXI_URL_TEMPLATE,
    YELLOW_TAXI_OUTPUTFILE_TEMPLATE,
    YELLOW_TAXI_GCS_DESTINATION_TEMPLATE
)


# instantiate DAG for for-hire vehicle trip data
FHV_URL_TEMPLATE = BASE_URL + '/trip-data/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_OUTPUTFILE_TEMPLATE = AIRFLOW_HOME + '/fhv_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_GCS_DESTINATION_TEMPLATE = 'raw/fhv_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

fhv_ingestion_dag = DAG(
    dag_id="fhv_ingestion",
    schedule_interval="@monthly",
    end_date=datetime(2019, 12, 31),
    default_args=default_args,
    tags=['dtc-de']
)

download_parquetize_upload_dag(
    fhv_ingestion_dag,
    FHV_URL_TEMPLATE,
    FHV_OUTPUTFILE_TEMPLATE,
    FHV_GCS_DESTINATION_TEMPLATE
)


# instantiate DAG for zone lookup data - will only be called once
ZONES_URL_TEMPLATE = BASE_URL + '/misc/taxi+_zone_lookup.csv'
ZONES_CSV_OUTPUTFILE = AIRFLOW_HOME + '/taxi_zone_lookup.csv'
ZONES_PARQUET_OUTPUTFILE = ZONES_CSV_OUTPUTFILE.replace('.csv', '.parquet')
ZONES_GCS_DESTINATION_TEMPLATE = 'raw/taxi_zone_lookup.parquet'

zone_lookup_ingestion_dag = DAG(
    dag_id="zone_lookup_ingestion",
    schedule_interval="@once",
    default_args=default_args,
    tags=['dtc-de']
)

download_parquetize_upload_dag(
    zone_lookup_ingestion_dag,
    ZONES_URL_TEMPLATE,
    ZONES_CSV_OUTPUTFILE,
    ZONES_GCS_DESTINATION_TEMPLATE,
    ZONES_PARQUET_OUTPUTFILE
)
