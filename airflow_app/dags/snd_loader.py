from datetime import datetime
import psycopg2

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from helpers.s3_helper import list_s3_files, download_from_s3

PROCESSED_BUCKET = "dataagritecta-processed"
PREFIX = "snd/"


def load(**context):
    conn = None
    cursor = None
    try:
        
         
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='postgres',
            user='postgres',
            password='postgres@123',
        )
        cursor = conn.cursor()
        
        files = list_s3_files(bucket=PROCESSED_BUCKET, prefix=PREFIX)
        print(f"Found {len(files)} files in {PROCESSED_BUCKET}/{PREFIX}")
    
        # Since snd_processor uploads one combined file, download and load that specific file
        key = "snd/snd_corn.csv"
        local_path = f"/tmp/{key.replace('/', '_')}"
        print(f"Downloading s3://{PROCESSED_BUCKET}/{key}")
        download_from_s3(bucket=PROCESSED_BUCKET, key=key, local_path=local_path)

        table_name = "snd_corn"

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Commodity_Code BIGINT,
            Commodity_Description TEXT,
            Country_Code TEXT,
            Country_Name TEXT,
            Market_Year BIGINT,
            Calendar_Year BIGINT,
            Month BIGINT,
            Attribute_ID BIGINT,
            Attribute_Description TEXT,
            Unit_ID BIGINT,
            Unit_Description TEXT,
            Value DOUBLE PRECISION
        );
        """
        cursor.execute(create_sql)
        conn.commit()

        # Insert data using COPY
        with open(local_path, 'r') as f:
            cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)
        conn.commit()

        print(f"Inserted data into table {table_name}")

    except psycopg2.Error as e:
        print(f"Database connection or operation error: {e}")
    except Exception as e:
        print(f"General error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("Done. Processed the combined file")


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
