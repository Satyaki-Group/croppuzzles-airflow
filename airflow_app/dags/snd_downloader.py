import io
import os
import zipfile
from datetime import datetime

import requests
from dateutil.rrule import MONTHLY, rrule

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from helpers.s3_helper import upload_to_s3, list_s3_files, list_s3_prefixes

RAW_BUCKET = "dataagritecta-raw"
S3_PREFIX = "snd"

BASE_URL = "https://apps.fas.usda.gov/psdonline/downloads/archives"

# Each archive zip contains one target CSV
ARCHIVES = {
    "psd_alldata_csv.zip": "psd_alldata.csv",
    "psd_grains_pulses_csv.zip": "psd_grains_pulses.csv",
    "psd_oilseeds_csv.zip": "psd_oilseeds.csv",
}


def _s3_key(year: int, month: int, csv_name: str) -> str:
    return f"{S3_PREFIX}/{year}/{month:02d}/{csv_name}"


def _find_resume_point(fallback_year: int) -> datetime:
    """
    Return the first month to download by finding the latest year/month
    prefix already present in S3 and returning the month after it.
    Falls back to January of fallback_year if nothing is uploaded yet.
    """
    fallback = datetime(fallback_year, 1, 1)

    year_prefixes = list_s3_prefixes(RAW_BUCKET, S3_PREFIX)
    if not year_prefixes:
        print(f"No data in S3 yet – starting from {fallback_year}/01")
        return fallback

    latest_year_prefix = max(year_prefixes)
    last_year = int(latest_year_prefix.rstrip("/").split("/")[-1])

    month_prefixes = list_s3_prefixes(RAW_BUCKET, latest_year_prefix)
    if not month_prefixes:
        print(f"No months under {latest_year_prefix} – starting from {fallback_year}/01")
        return fallback

    last_month = int(max(month_prefixes).rstrip("/").split("/")[-1])
    print(f"Last month in S3: {last_year}/{last_month:02d} – resuming from next month")

    if last_month == 12:
        return datetime(last_year + 1, 1, 1)
    return datetime(last_year, last_month + 1, 1)


def download_snd(start_year: int = 2006, end_year: int = 2026, **context):
    start = _find_resume_point(fallback_year=start_year)
    end = datetime(end_year, 12, 31)

    for dt in rrule(MONTHLY, dtstart=start, until=end):
        year, month = dt.year, dt.month
        existing_month_keys = set(list_s3_files(RAW_BUCKET, f"{S3_PREFIX}/{year}/{month:02d}/"))

        for archive_name, csv_name in ARCHIVES.items():
            s3_key = _s3_key(year, month, csv_name)
            if s3_key in existing_month_keys:
                print(f"  ✓ {s3_key} exists, skipping")
                continue

            url = f"{BASE_URL}/{year}/{month:02d}/{archive_name}"
            print(f"  Downloading {url} ...", end=" ")

            try:
                response = requests.get(url, timeout=60)
                response.raise_for_status()
            except requests.HTTPError as e:
                print(f"HTTP {response.status_code} – skipping")
                continue
            except requests.RequestException as e:
                print(f"ERROR: {e} – skipping")
                continue

            try:
                with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                    if csv_name not in zf.namelist():
                        print(f"'{csv_name}' not found in archive – skipping")
                        continue
                    csv_bytes = zf.read(csv_name)
            except zipfile.BadZipFile:
                print("bad zip – skipping")
                continue

            local_path = f"/tmp/{year}{month:02d}_{csv_name}"
            with open(local_path, "wb") as f:
                f.write(csv_bytes)

            upload_to_s3(local_path=local_path, bucket=RAW_BUCKET, key=s3_key)
            os.remove(local_path)
            print(f"uploaded → s3://{RAW_BUCKET}/{s3_key}")


with DAG(
    dag_id="snd_downloader",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * 1",
    catchup=False,
    tags=["downloader", "snd"],
) as dag:
    PythonOperator(
        task_id="download_snd",
        python_callable=download_snd,
        op_kwargs={"start_year": 2006, "end_year": 2026},
    )
