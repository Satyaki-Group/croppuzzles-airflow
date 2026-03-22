from datetime import datetime

import pandas as pd

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from helpers.s3_helper import list_s3_files, download_from_s3, upload_to_s3

RAW_BUCKET = "dataagritecta-raw"
PROCESSED_BUCKET = "dataagritecta-processed"
PREFIX = "snd/"


def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    
    # Clean up string columns
    df['Unit_Description'] = df['Unit_Description'].str.replace('[()]', '', regex=True).str.strip()
    df['Attribute_Description'] = df['Attribute_Description'].str.strip().str.title()
    df['Commodity_Description'] = df['Commodity_Description'].str.strip().str.title()
    
    # Fill missing country codes 
    mapping = df.dropna(subset=['Country_Code']).drop_duplicates(subset=['Country_Name']).set_index('Country_Name')['Country_Code']
    df['Country_Code'] = df['Country_Code'].fillna(df['Country_Name'].map(mapping))
    df['Country_Code'] = df['Country_Code'].fillna(df['Country_Name'].map({'Netherlands Antilles': 'AN'}))

    # Force expected data types where possible (nullable ints + strings)
    dtype_map = {
        "Commodity_Code": "Int64",
        "Commodity_Description": "string",
        "Country_Code": "string",
        "Country_Name": "string",
        "Market_Year": "Int64",
        "Calendar_Year": "Int64",
        "Month": "Int64",
        "Attribute_ID": "Int64",
        "Attribute_Description": "string",
        "Unit_ID": "Int64",
        "Unit_Description": "string",
        "Value": "float64",
    }

    for col, dtype in dtype_map.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except Exception:
                pass
    
    df.duplicated().sum()

    return df


def process(**context):
    files = list_s3_files(bucket=RAW_BUCKET, prefix=PREFIX)
    print(f"Found {len(files)} files in {RAW_BUCKET}/{PREFIX}")

    dfs = []

    for key in files:
        local_path = f"/tmp/{key.replace('/', '_')}"
        print(f"Downloading s3://{RAW_BUCKET}/{key}")
        download_from_s3(bucket=RAW_BUCKET, key=key, local_path=local_path)

        df = pd.read_csv(local_path)
        df = df[df['Commodity_Description'].str.contains('corn|soybean', case=False, na=False)]
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)

    # Pass the combined dataframe to transform function
    df = transform_df(df)

    
    local_path = "/tmp/snd_corn.csv"
    df.to_csv(local_path, index=False)

    print(f"Uploading to s3://{PROCESSED_BUCKET}/snd/snd_corn.csv")
    upload_to_s3(local_path=local_path, bucket=PROCESSED_BUCKET, key="snd/snd_corn.csv")

    print("Done.")


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
