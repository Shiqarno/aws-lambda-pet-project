import json
import os
import subprocess
import tempfile
import time
import zipfile
from typing import Any, Dict, Optional

import boto3
from hydra import compose, initialize

s3 = boto3.client("s3")


def create_s3_bucket_cli(bucket_name: str, region: str = "us-east-1") -> None:
    """Create an S3 bucket using the AWS CLI.

    Args:
        bucket_name (str): The name of the bucket to create.
        region (str): The AWS region to create the bucket in.
    """
    try:
        if region == "us-east-1":
            cmd = ["aws", "s3api", "create-bucket", "--bucket", bucket_name]
        else:
            cmd = [
                "aws",
                "s3api",
                "create-bucket",
                "--bucket",
                bucket_name,
                "--region",
                region,
                "--create-bucket-configuration",
                f"LocationConstraint={region}",
            ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Bucket '{bucket_name}' created successfully.")
        else:
            print(f"❌ Error creating bucket:\n{result.stderr}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def create_s3_folder(bucket_name: str, folder_name: str) -> None:
    """Create a folder (zero-byte object with '/' suffix) in an S3 bucket.

    Args:
        bucket_name (str): Name of the bucket.
        folder_name (str): Folder path inside the bucket.
    """
    if not folder_name.endswith("/"):
        folder_name += "/"
    try:
        s3.put_object(Bucket=bucket_name, Key=folder_name)
        print(f"✅ Folder '{folder_name}' created in bucket '{bucket_name}'.")
    except Exception as e:
        print(f"❌ Error creating folder: {e}")


def set_s3_lifecycle_expiration(
    bucket_name: str, prefix: str = "archive/", days: int = 1
) -> None:
    """Set S3 lifecycle configuration to expire files under a specific prefix.

    Args:
        bucket_name (str): Name of the S3 bucket.
        prefix (str): Prefix for files to expire.
        days (int): Number of days after which files should be deleted.
    """
    lifecycle_configuration = {
        "Rules": [
            {
                "ID": "DeleteArchiveAfter1Day",
                "Prefix": prefix,
                "Status": "Enabled",
                "Expiration": {"Days": days},
            }
        ]
    }
    try:
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration=lifecycle_configuration
        )
        print(f"✅ Lifecycle rule set: delete files in '{prefix}' after {days} day(s).")
    except Exception as e:
        print(f"❌ Failed to set lifecycle rule: {e}")


def create_iam_role_for_lambda_cli(role_name: str) -> Optional[str]:
    """Create an IAM role for AWS Lambda with S3 and CloudWatch access.

    Args:
        role_name (str): Name of the IAM role.

    Returns:
        Optional[str]: The ARN of the created IAM role, or None on failure.
    """
    role_arn = None
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    try:
        with tempfile.NamedTemporaryFile(
            mode="w+", delete=False, suffix=".json"
        ) as tmp_file:
            json.dump(trust_policy, tmp_file)
            tmp_file_path = tmp_file.name
        create_cmd = [
            "aws",
            "iam",
            "create-role",
            "--role-name",
            role_name,
            "--assume-role-policy-document",
            f"file://{tmp_file_path}",
            "--description",
            "IAM role for AWS Lambda with S3 and CloudWatch access",
        ]
        result = subprocess.run(create_cmd, capture_output=True, text=True)
        response = json.loads(result.stdout)
        role_arn = response["Role"]["Arn"]
        for policy_arn in [
            "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
            "arn:aws:iam::aws:policy/AmazonS3FullAccess",
        ]:
            subprocess.run(
                [
                    "aws",
                    "iam",
                    "attach-role-policy",
                    "--role-name",
                    role_name,
                    "--policy-arn",
                    policy_arn,
                ],
                capture_output=True,
                text=True,
            )
        print(f"✅ IAM role '{role_name}' created and policies attached.")
    finally:
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
    return role_arn


def zip_lambda_function(source_file: str, zip_file: str) -> None:
    """Zip the Lambda function source file.

    Args:
        source_file (str): Path to the Python file to zip.
        zip_file (str): Output zip file path.
    """
    with zipfile.ZipFile(zip_file, "w") as z:
        z.write(source_file, arcname=os.path.basename(source_file))
    print(f"✅ '{source_file}' -> '{zip_file}'")


def create_lambda_function_from_py(
    function_name: str,
    source_file: str,
    role_arn: str,
    handler_name: str = "lambda_function.lambda_handler",
    runtime: str = "python3.13",
    region: str = "us-east-1",
) -> Optional[str]:
    """Create a Lambda function from a Python file.

    Args:
        function_name (str): Name of the Lambda function.
        source_file (str): Path to the source Python file.
        role_arn (str): ARN of the IAM role to assign to the Lambda.
        handler_name (str): Function handler name.
        runtime (str): Python runtime version.
        region (str): AWS region.

    Returns:
        Optional[str]: ARN of the created Lambda function.
    """
    zip_file = "function.zip"
    zip_lambda_function(source_file, zip_file)
    lambda_client = boto3.client("lambda", region_name=region)
    with open(zip_file, "rb") as f:
        zipped_code = f.read()
    try:
        response = lambda_client.create_function(
            FunctionName=function_name,
            Runtime=runtime,
            Role=role_arn,
            Handler=handler_name,
            Code={"ZipFile": zipped_code},
            Description="Lambda created from Python file",
            Timeout=10,
            MemorySize=128,
            Publish=True,
        )
        print(f"✅ Lambda function '{function_name}' created successfully.")
        os.remove(zip_file)
        return response["FunctionArn"]
    except lambda_client.exceptions.ResourceConflictException:
        print(f"⚠️ Lambda function '{function_name}' already exists.")
    except Exception as e:
        print(f"❌ Error creating Lambda function: {e}")
    if os.path.exists(zip_file):
        os.remove(zip_file)
    return None


def update_lambda_env_variables(cfg: Dict[str, Any]) -> None:
    """
    Update AWS Lambda environment variables for the specified function.

    Args:
        cfg (Dict[str, Any]): configuration settings.

    Returns:
        None
    """
    lambda_client = boto3.client("lambda", region_name=cfg.aws.region)

    # Define the new environment variables
    new_env_vars = {
        "bucket_name": cfg.s3.bucket_name,
        "incoming_folder": cfg.s3.incoming_folder,
        "archive_folder": cfg.s3.archive_folder,
    }

    # Fetch existing environment variables and merge
    current_config = lambda_client.get_function_configuration(
        FunctionName=cfg.func.name
    )
    existing_vars = current_config.get("Environment", {}).get("Variables", {})
    existing_vars.update(new_env_vars)

    # Update function configuration
    try:
        lambda_client.update_function_configuration(
            FunctionName=cfg.func.name, Environment={"Variables": existing_vars}
        )
        print(f"✅ Environment variables updated for '{cfg.func.name}'.")
    except Exception as e:
        print(f"❌ Failed to update environment variables: {e}")


def add_pandas_layer_to_lambda(
    function_name: str, layer_arn: str, region: str = "us-east-1"
) -> None:
    """Attach a Pandas layer to an existing Lambda function.

    Args:
        function_name (str): Name of the Lambda function.
        layer_arn (str): ARN of the Pandas layer.
        region (str): AWS region.
    """
    lambda_client = boto3.client("lambda", region_name=region)
    response = lambda_client.get_function_configuration(FunctionName=function_name)
    existing_layer_arns = [layer["Arn"] for layer in response.get("Layers", [])]
    updated_layers = existing_layer_arns + [layer_arn]
    try:
        lambda_client.update_function_configuration(
            FunctionName=function_name, Layers=updated_layers
        )
        print(f"✅ Layer {layer_arn} has been added to function '{function_name}'.")
    except Exception as e:
        print(f"❌ Failed to add layer: {e}")


def add_s3_trigger_to_lambda(
    bucket_name: str,
    lambda_function_arn: str,
    prefix: str = "",
    region: str = "us-east-1",
) -> None:
    """Add an S3 event trigger to a Lambda function.

    Args:
        bucket_name (str): Name of the S3 bucket.
        lambda_function_arn (str): ARN of the Lambda function.
        prefix (str): Key prefix filter for object-created events.
        region (str): AWS region.
    """
    lambda_client = boto3.client("lambda", region_name=region)
    function_name = lambda_function_arn.split(":")[-1]
    try:
        lambda_client.add_permission(
            FunctionName=function_name,
            StatementId=f"{bucket_name}-s3-trigger",
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
            SourceArn=f"arn:aws:s3:::{bucket_name}",
        )
        print(f"✅ {function_name}: Lambda permission granted to S3.")
    except lambda_client.exceptions.ResourceConflictException:
        print(f"⚠️ {function_name}: Permission already exists (StatementId conflict).")
    notification_configuration = {
        "LambdaFunctionConfigurations": [
            {
                "LambdaFunctionArn": lambda_function_arn,
                "Events": ["s3:ObjectCreated:Put"],
                "Filter": {
                    "Key": {"FilterRules": [{"Name": "prefix", "Value": prefix}]}
                }
                if prefix
                else {},
            }
        ]
    }
    try:
        s3.put_bucket_notification_configuration(
            Bucket=bucket_name, NotificationConfiguration=notification_configuration
        )
        print(f"✅ S3 event trigger created: on upload to '{bucket_name}/{prefix}'")
    except Exception as e:
        print(f"❌ Failed to configure trigger: {e}")


if __name__ == "__main__":
    # Initialize Hydra configuration
    with initialize(version_base=None, config_path=".", job_name="app"):
        cfg = compose(config_name="settings")

        # Step 1: Create S3 resources (bucket and folders)
        create_s3_bucket_cli(cfg.s3.bucket_name, region=cfg.aws.region)
        create_s3_folder(cfg.s3.bucket_name, cfg.s3.incoming_folder)
        create_s3_folder(cfg.s3.bucket_name, cfg.s3.archive_folder)

        # Step 2: Configure lifecycle rule to expire archived files
        set_s3_lifecycle_expiration(cfg.s3.bucket_name, prefix=cfg.s3.archive_folder)

        # Step 3: Create IAM role for Lambda
        role_arn = create_iam_role_for_lambda_cli(cfg.aws.role_name)

        # Wait a few seconds to ensure the IAM role is fully propagated
        time.sleep(5)

        # Step 4: Deploy Lambda function from source file
        function_arn = create_lambda_function_from_py(
            function_name=cfg.func.name,
            source_file=cfg.func.file_path,
            role_arn=role_arn,
        )

        # Wait a few seconds to ensure the Lambda function is ready
        time.sleep(5)

        # Step 4.1: Set environment variables
        update_lambda_env_variables(cfg)

        # Wait a few seconds more
        time.sleep(5)

        # Step 5: Add Pandas layer to Lambda function
        add_pandas_layer_to_lambda(
            cfg.func.name, cfg.func.pandas_layer_arn, cfg.aws.region
        )

        # Step 6: Set up S3 trigger to invoke Lambda on file upload
        add_s3_trigger_to_lambda(
            bucket_name=cfg.s3.bucket_name,
            lambda_function_arn=function_arn,
            prefix=cfg.s3.incoming_folder + "/",
        )
