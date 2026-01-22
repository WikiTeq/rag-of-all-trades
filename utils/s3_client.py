import boto3
from botocore.exceptions import ClientError
from utils.config import settings

def get_s3_client(
    bucket: str = None,
    endpoint: str = None,
    access_key: str = None,
    secret_key: str = None,
    region: str = None,
    use_ssl: bool = True
):
    """
    Return S3 client + bucket.
    If params are None, fallback to first S3 source in settings.SOURCES.
    """
    try:
        # Use first S3 source if any parameter is missing
        if not all([bucket, endpoint, access_key, secret_key]):
            s3_sources = [s for s in settings.SOURCES if s["type"] == "s3"]
            if not s3_sources:
                raise RuntimeError("No S3 sources configured in settings.")
            
            source = s3_sources[0]
            cfg = source["config"]
            bucket = bucket or cfg.get("bucket_override")
            endpoint = endpoint or cfg.get("endpoint")
            access_key = access_key or cfg.get("access_key")
            secret_key = secret_key or cfg.get("secret_key")
            region = region or cfg.get("region")
            use_ssl = use_ssl if use_ssl is not None else cfg.get("use_ssl", True)

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
