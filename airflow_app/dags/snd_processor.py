from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from helpers.s3_helper import list_s3_files, download_from_s3, upload_to_s3

RAW_BUCKET = "dataagritecta-raw"
PROCESSED_BUCKET = "dataagritecta-processed"
PREFIX = "snd/"


def process(**context):
    files = list_s3_files(bucket=RAW_BUCKET, prefix=PREFIX)
    print(f"Found {len(files)} files in {RAW_BUCKET}/{PREFIX}")

    for key in files:
        local_path = f"/tmp/{key.replace('/', '_')}"
        print(f"Downloading s3://{RAW_BUCKET}/{key}")
        download_from_s3(bucket=RAW_BUCKET, key=key, local_path=local_path)

        print(f"Uploading to s3://{PROCESSED_BUCKET}/{key}")
        upload_to_s3(local_path=local_path, bucket=PROCESSED_BUCKET, key=key)

    print(f"Done. Moved {len(files)} files to {PROCESSED_BUCKET}")


with DAG(
    dag_id="snd_processor",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["processor", "snd"],
) as dag:
    PythonOperator(
        task_id="process",
        python_callable=process,
    )
