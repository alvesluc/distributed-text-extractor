
import boto3
from botocore.exceptions import ClientError

S3_CONFIG = {
    "endpoint_url": "http://localhost:9000",
    "aws_access_key_id": "arqcabe_os",
    "aws_secret_access_key": "arqcabe_os",
    "region_name": "us-east-1"
}

def get_s3_client():
    return boto3.client('s3', **S3_CONFIG)

def create_bucket_if_not_exists(s3_client, bucket_name: str):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError:
        s3_client.create_bucket(Bucket=bucket_name)