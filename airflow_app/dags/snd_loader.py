from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from helpers.s3_helper import list_s3_files, download_from_s3, upload_to_s3

PROCESSED_BUCKET = "dataagritecta-processed"
LOADER_BUCKET = "dataagritecta-loaded"
PREFIX = "snd/"


def load(**context):
    files = list_s3_files(bucket=PROCESSED_BUCKET, prefix=PREFIX)
    print(f"Found {len(files)} files in {PROCESSED_BUCKET}/{PREFIX}")

    for key in files:
        local_path = f"/tmp/{key.replace('/', '_')}"
        print(f"Downloading s3://{PROCESSED_BUCKET}/{key}")
        download_from_s3(bucket=PROCESSED_BUCKET, key=key, local_path=local_path)

        print(f"Loading to s3://{LOADER_BUCKET}/{key}")
        upload_to_s3(local_path=local_path, bucket=LOADER_BUCKET, key=key)

    print(f"Done. Loaded {len(files)} files to {LOADER_BUCKET}")


with DAG(
    dag_id="snd_loader",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["loader", "snd"],
) as dag:
    PythonOperator(
        task_id="load",
        python_callable=load,
    )
