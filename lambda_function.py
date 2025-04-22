import io

import boto3
import pandas as pd

s3 = boto3.client("s3")


def s3_file_exist(bucket_name, destination_path):
    try:
        s3.head_object(Bucket=bucket_name, Key=destination_path)
        return True
    except Exception:
        return False


def lambda_handler(event, context):
    bucket_name = "aws-lambda-pet-project-bucket"
    incoming_folder = "incoming/"
    archive_folder = "archive/"

    source_path = event["Records"][0]["s3"]["object"]["key"]

    # Filter
    if not (source_path.startswith(incoming_folder) and source_path.endswith(".csv")):
        return

    destination_path = source_path.replace(incoming_folder, archive_folder)
    destination_path = destination_path.replace(".csv", ".parquet")

    # Idempotent result
    if s3_file_exist(bucket_name, destination_path):
        s3.delete_object(Bucket=bucket_name, Key=source_path)
        print(f"✅ {source_path}: already processed")
        return

    response = s3.get_object(Bucket=bucket_name, Key=source_path)

    body = response["Body"]
    df = pd.read_csv(body)

    # Convert to Parquet in-memory
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    # Upload to S3
    s3.upload_fileobj(buffer, Bucket=bucket_name, Key=destination_path)

    s3.delete_object(Bucket=bucket_name, Key=source_path)

    print(f"✅ {source_path} -> {destination_path} ({len(df)} rows)")
