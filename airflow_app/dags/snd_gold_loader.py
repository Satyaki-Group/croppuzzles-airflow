from datetime import datetime
import os
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from helpers.s3_helper import download_from_s3

PROCESSED_BUCKET = "dataagritecta-processed"
YEAR = "2025"
MONTH = "01"


def load(**context):
    conn = None
    cursor = None
    local_path = f"/tmp/snd_{YEAR}_{MONTH}_combined_cleaned.parquet"
    try:
        key = f"snd/{YEAR}/{MONTH}/combined_cleaned.parquet"
        print(f"Downloading s3://{PROCESSED_BUCKET}/{key}")
        download_from_s3(bucket=PROCESSED_BUCKET, key=key, local_path=local_path)

        df = pd.read_parquet(local_path)
        print(f"Loaded {len(df)} rows from parquet")

        conn = psycopg2.connect(
            host=os.environ['DB_HOST'],
            port=int(os.environ['DB_PORT']),
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
        )
        cursor = conn.cursor()

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

        columns = [
            "Commodity_Code", "Commodity_Description", "Country_Code", "Country_Name",
            "Market_Year", "Calendar_Year", "Month", "Attribute_ID",
            "Attribute_Description", "Unit_ID", "Unit_Description", "Value",
        ]
        # Convert nullable pandas types (Int64, string) to plain Python objects
        # so psycopg2 can handle them; replace pd.NA with None
        df_insert = df[columns].astype(object).where(df[columns].notna(), None)
        rows = list(df_insert.itertuples(index=False, name=None))
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
        execute_values(cursor, insert_sql, rows)
        conn.commit()

        print(f"Inserted {len(rows)} rows into table {table_name}")

    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print(f"Done. Processed snd/{YEAR}/{MONTH}")
        return "Success"


with DAG(
    dag_id="snd_gold_loader",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["gold_loader", "snd"],
) as dag:
    PythonOperator(
        task_id="load",
        python_callable=load,
    )
