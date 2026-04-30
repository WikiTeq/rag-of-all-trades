import boto3
from botocore.exceptions import ClientError


def get_s3_client(
    bucket: str = None,
    endpoint: str = None,
    access_key: str = None,
    secret_key: str = None,
    region: str = None,
    use_ssl: bool = True,
):
    """Return S3 client + bucket."""
    try:
        # Initialize boto3 client
        s3 = boto3.client(
            service_name="s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            use_ssl=use_ssl,
        )
        return s3, bucket
    except ClientError as e:
        raise RuntimeError(f"Failed to initialize S3 client: {e}")
