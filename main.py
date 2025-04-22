import json
import os
import subprocess
import tempfile
import time
import zipfile

import boto3
from hydra import compose, initialize


def create_s3_bucket_cli(bucket_name, region="us-east-1"):
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
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Error creating bucket:\n{result.stderr}")

    except Exception as e:
        print(f"Unexpected error: {e}")


def create_s3_folder(bucket_name, folder_name):
    if not folder_name.endswith("/"):
        folder_name += "/"

    s3 = boto3.client("s3")

    try:
        s3.put_object(Bucket=bucket_name, Key=folder_name)
        print(f"Folder '{folder_name}' created in bucket '{bucket_name}'.")
    except Exception as e:
        print(f"Error creating folder: {e}")


def set_s3_lifecycle_expiration(bucket_name, prefix="archive/", days=1):
    s3 = boto3.client("s3")

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


def create_iam_role_for_lambda_cli(role_name):
    role_arn = None

    # Define the trust policy
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
        # Save the trust policy to a temporary JSON file
        with tempfile.NamedTemporaryFile(
            mode="w+", delete=False, suffix=".json"
        ) as tmp_file:
            json.dump(trust_policy, tmp_file)
            tmp_file_path = tmp_file.name

        # Step 1: Create the role
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
        print(result.stdout if result.returncode == 0 else result.stderr)

        response = json.loads(result.stdout)
        role_arn = response["Role"]["Arn"]

        # Step 2: Attach policies to the role
        policies = [
            "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
            "arn:aws:iam::aws:policy/AmazonS3FullAccess",
        ]
        for policy_arn in policies:
            attach_cmd = [
                "aws",
                "iam",
                "attach-role-policy",
                "--role-name",
                role_name,
                "--policy-arn",
                policy_arn,
            ]
            subprocess.run(attach_cmd, capture_output=True, text=True)

        print(f"✅ IAM role '{role_name}' created and policies attached.")

    finally:
        # Clean up the temporary file
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)

    return role_arn


def zip_lambda_function(source_file, zip_file):
    with zipfile.ZipFile(zip_file, "w") as z:
        z.write(source_file, arcname=os.path.basename(source_file))
    print(f"✅ '{source_file}' -> '{zip_file}'")


def create_lambda_function_from_py(
    function_name,
    source_file,
    role_arn,
    handler_name="lambda_function.lambda_handler",
    runtime="python3.13",
    region="us-east-1",
):
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
        return response["FunctionArn"]

    except lambda_client.exceptions.ResourceConflictException:
        print(f"⚠️ Lambda function '{function_name}' already exists.")
    except Exception as e:
        print(f"❌ Error creating Lambda function: {e}")

    if os.path.exists(zip_file):
        os.remove(zip_file)


def add_s3_trigger_to_lambda(
    bucket_name, lambda_function_arn, prefix="", region="us-east-1"
):
    s3 = boto3.client("s3")
    lambda_client = boto3.client("lambda", region_name=region)

    # Extract function name from ARN
    function_name = lambda_function_arn.split(":")[-1]

    # Grant S3 permission to invoke Lambda
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

    # Set the event notification on the bucket
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
    with initialize(version_base=None, config_path=".", job_name="app"):
        role_arn = None
        function_arn = None
        cfg = compose(config_name="env")
        create_s3_bucket_cli(cfg.s3.bucket_name, region=cfg.aws.region)
        create_s3_folder(cfg.s3.bucket_name, cfg.s3.incoming_folder)
        create_s3_folder(cfg.s3.bucket_name, cfg.s3.archive_folder)
        set_s3_lifecycle_expiration(cfg.s3.bucket_name, prefix=cfg.s3.archive_folder)

        role_arn = create_iam_role_for_lambda_cli(cfg.aws.role_name)
        if role_arn is None:
            pass  # TODO: error output
        print(role_arn)

        time.sleep(5)

        function_arn = create_lambda_function_from_py(
            function_name=cfg.func.name,
            source_file=cfg.func.file_path,
            role_arn=role_arn,
        )

        time.sleep(5)

        add_s3_trigger_to_lambda(
            bucket_name=cfg.s3.bucket_name,
            lambda_function_arn=function_arn,
            prefix=cfg.s3.incoming_folder + "/",
        )
