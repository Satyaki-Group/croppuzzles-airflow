import os
from datetime import datetime

import pandas as pd
from pathlib import PurePosixPath
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from helpers.s3_helper import list_s3_prefixes, download_from_s3, upload_df_to_s3

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


CSV_FILES = ["psd_oilseeds.csv", "psd_alldata.csv", "psd_grains_pulses.csv"]


def read_csv(local_dir: str) -> pd.DataFrame:
    dfs = []
    for filename in CSV_FILES:
        df = pd.read_csv(os.path.join(local_dir, filename))
        df = df[df['Commodity_Description'].str.contains('corn|soybean', case=False, na=False)]
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


def process(**context):
    year_prefixes = list_s3_prefixes(bucket=RAW_BUCKET, prefix=PREFIX)
    print(f"Found {len(year_prefixes)} year(s) under {RAW_BUCKET}/{PREFIX}")

    for year_prefix in year_prefixes:
        year = year_prefix.rstrip("/").split("/")[-1]
        if year != "2025":
            continue

        month_prefixes = list_s3_prefixes(bucket=RAW_BUCKET, prefix=year_prefix)

        for month_prefix in month_prefixes:
            # month_prefix looks like "snd/2025/01/"
            parts = month_prefix.rstrip("/").split("/")
            year, month = parts[-2], parts[-1]

            local_dir = f"/tmp/snd_{year}_{month}"
            os.makedirs(local_dir, exist_ok=True)

            # Download all 3 CSVs into local_dir
            for filename in CSV_FILES:
                s3_key = f"{month_prefix}{filename}"
                local_file = os.path.join(local_dir, filename)
                print(f"Downloading s3://{RAW_BUCKET}/{s3_key}")
                download_from_s3(bucket=RAW_BUCKET, key=s3_key, local_path=local_file)

            # Concatenate
            df_combined = read_csv(local_dir)

            # Transform
            df_transformed = transform_df(df_combined)

            # Upload to matching path in processed bucket
            remote_path = str(PurePosixPath("snd", year, month, "combined_cleaned.parquet"))
            print(f"Uploading to s3://{PROCESSED_BUCKET}/{remote_path}")
            upload_df_to_s3(df_transformed, PROCESSED_BUCKET, remote_path)

            print(f"Done: {year}/{month}")


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
