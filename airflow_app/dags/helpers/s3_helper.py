import os
import boto3
import pandas as pd
from io import BytesIO

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        # aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
    )


def download_from_s3(bucket: str, key: str, local_path: str) -> None:
    client = get_s3_client()
    client.download_file(bucket, key, local_path)


def list_s3_files(bucket: str, prefix: str = "") -> list[str]:
    client = get_s3_client()
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj["Key"] for obj in response.get("Contents", [])]


def list_s3_prefixes(bucket: str, prefix: str = "") -> list[str]:
    """Return immediate sub-prefixes (like ls on a directory).

    E.g. list_s3_prefixes(bucket, "usda/psd/") returns
    ["usda/psd/2006/", "usda/psd/2007/", ...]
    """
    client = get_s3_client()
    prefix = prefix if prefix.endswith("/") else prefix + "/"
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    return [cp["Prefix"] for cp in response.get("CommonPrefixes", [])]


def upload_to_s3(local_path: str, bucket: str, key: str) -> None:
    client = get_s3_client()
    client.upload_file(local_path, bucket, key)

def upload_df_to_s3(df: pd.DataFrame, bucket: str, key: str):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    client = get_s3_client()
    client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())