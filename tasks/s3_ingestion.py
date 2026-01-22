import io
import re
import logging
import unicodedata
from markitdown import MarkItDown
from tasks.base import IngestionJob
from utils.s3_client import get_s3_client
from tasks.helper_classes.ingestion_item import IngestionItem

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

class S3IngestionJob(IngestionJob):
    
    @property
    def source_type(self) -> str:
        return "s3"

    def __init__(self, config):
        super().__init__(config)

        cfg = config.get("config", {})

        buckets = cfg.get("buckets", [])

        if isinstance(buckets, str):
            buckets = [b.strip() for b in buckets.split(",") if b.strip()]
        self.buckets = buckets or []

        # Initialize S3 client - access nested config dict
        client_params = {
            "endpoint": cfg.get("endpoint"),
            "access_key": cfg.get("access_key"),
            "secret_key": cfg.get("secret_key"),
            "region": cfg.get("region"),
            "use_ssl": cfg.get("use_ssl", True),
        }
        self.s3_client, _ = get_s3_client(**client_params)

        # Markdown parser
        self.md = MarkItDown()

    def sanitize_s3_key(self, key: str) -> str:
        key = unicodedata.normalize("NFKD", key)
        key = key.encode("ascii", "ignore").decode("ascii")
        key = re.sub(r"[ \\/]+", "_", key)
        key = re.sub(r"[^a-zA-Z0-9\-_\.]", "", key)
        return key[:255]

    def list_items(self):
        """
        Generator that yields S3 items one at a time to avoid loading
        all items into memory at once (critical for large buckets).
        """
        for bucket in self.buckets:
            continuation_token = None
            while True:
                try:
                    params = {"Bucket": bucket, "MaxKeys": 1000}
                    if continuation_token:
                        params["ContinuationToken"] = continuation_token

                    resp = self.s3_client.list_objects_v2(**params)
                    contents = resp.get("Contents", [])

                    # Yield items one at a time
                    for obj in contents:
                        if not obj["Key"].endswith("/"):
                            yield IngestionItem(
                                id=f"s3://{bucket}/{obj['Key']}",
                                source_ref=(bucket, obj["Key"]),
                                last_modified=obj["LastModified"],
                            )

                    # Check if there are more objects
                    if resp.get("IsTruncated"):
                        continuation_token = resp.get("NextContinuationToken")
                    else:
                        break

                except Exception as e:
                    logger.error(f"[{bucket}] Failed to list objects: {e}")
                    break

    def get_raw_content(self, item: IngestionItem):
        bucket, key = item.source_ref
        try:
            obj = self.s3_client.get_object(Bucket=bucket, Key=key)
            content_bytes = obj["Body"].read()
            stream = io.BytesIO(content_bytes)
            try:
                result = self.md.convert_stream(stream)
                text = result.text_content or ""
                if text.strip():
                    logger.debug(f"[{bucket}/{key}] Converted to markdown successfully")
                    return text
                else:
                    logger.debug(f"[{bucket}/{key}] Empty markdown result, falling back to raw text")
                    return content_bytes.decode("utf-8", errors="ignore")
            except Exception as conversion_error:
                logger.warning(f"[{bucket}/{key}] Markdown conversion failed: {conversion_error}. Using raw text.")
                return content_bytes.decode("utf-8", errors="ignore")
        except Exception as e:
            logger.error(f"[{bucket}/{key}] Failed to fetch content: {e}")
            return ""

    def get_item_name(self, item: IngestionItem):
        _, key = item.source_ref
        return self.sanitize_s3_key(key)