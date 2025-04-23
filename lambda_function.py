import io
from typing import Any, Dict

import boto3
import pandas as pd

# Initialize the S3 client
s3 = boto3.client("s3")

# Constants for S3 bucket and folder paths
bucket_name = "aws-lambda-pet-project-bucket"
incoming_folder = "incoming/"
archive_folder = "archive/"


def s3_file_exist(destination_path: str) -> bool:
    """
    Check if a file exists in the specified S3 bucket.

    Args:
        bucket_name (str): Name of the S3 bucket.
        destination_path (str): Full path of the file within the bucket.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    try:
        s3.head_object(Bucket=bucket_name, Key=destination_path)
        return True
    except Exception:
        return False


def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    """
    AWS Lambda function handler that processes CSV files from S3,
    converts them to Parquet, uploads the result, and archives the original file.

    Args:
        event (Dict[str, Any]): Event data passed by AWS Lambda.
        context (Any): Lambda context object (unused).

    Returns:
        None
    """
    source_path = event["Records"][0]["s3"]["object"]["key"]

    # Filter non-CSV or non-incoming folder files
    if not (source_path.startswith(incoming_folder) and source_path.endswith(".csv")):
        return

    destination_path = source_path.replace(incoming_folder, archive_folder).replace(
        ".csv", ".parquet"
    )

    # Skip processing if file was already handled
    if s3_file_exist(destination_path):
        s3.delete_object(Bucket=bucket_name, Key=source_path)
        print(f"✅ {source_path}: already processed")
        return

    # Read CSV file from S3
    response = s3.get_object(Bucket=bucket_name, Key=source_path)
    body = response["Body"]
    df = pd.read_csv(body)

    # Convert DataFrame to Parquet in-memory
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    # Upload the Parquet file to S3
    s3.upload_fileobj(buffer, Bucket=bucket_name, Key=destination_path)

    # Delete the original CSV file
    s3.delete_object(Bucket=bucket_name, Key=source_path)

    # Confirmation - can be observed in CloudWatch
    print(f"✅ {source_path} -> {destination_path} ({len(df)} rows)")
