# Standard library imports
import hashlib
import logging
import re
from datetime import datetime
from typing import Dict, Any, Iterator

# Third-party imports
from llama_index.readers.mediawiki import MediaWikiReader

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class MediaWikiIngestionJob(IngestionJob):

    @property
    def source_type(self) -> str:
        return "mediawiki"

    def __init__(self, config):
        """Initialize the MediaWiki ingestion job.

        Args:
            config: Configuration dictionary containing:
                - config.api_url: MediaWiki API endpoint URL (required)
                - config.user_agent: User-Agent header for requests (optional)
                - config.request_delay: Delay between requests in seconds (optional, default 0.1, must be >= 0)
                - config.page_limit: Pages per API call (optional, default 500, must be > 0)
                - config.batch_size: Pages to batch for timestamp fetching (optional, default 50, must be > 0)
                - config.max_retries: Maximum API request retries (optional, default 3, must be >= 0)
                - config.timeout: HTTP request timeout in seconds (optional, default 30, must be > 0)
                - config.namespaces: List of namespace IDs to include (optional, None = all namespaces)

        Raises:
            ValueError: If api_url is not provided or numeric config values are invalid
        """
        super().__init__(config)

        cfg = config.get("config", {})

        # Build the reader
        self._reader = MediaWikiReader(
            api_url=cfg.get("api_url", ""),
            user_agent=cfg.get("user_agent", "rag-of-all-trades-connector-mediawiki/1.0"),
            request_delay=cfg.get("request_delay", 0.1),
            page_limit=cfg.get("page_limit", 500),
            batch_size=cfg.get("batch_size", 50),
            max_retries=cfg.get("max_retries", 3),
            timeout=cfg.get("timeout", 30),
            namespaces=cfg.get("namespaces"),
            schedules=cfg.get("schedules", 3600),
        )

        logger.info(f"Initialized MediaWiki connector for {self._reader.api_url}")

    def list_items(self) -> Iterator[IngestionItem]:
        """Discover all pages in the MediaWiki instance and yield ingestion items.

        Iterates through pages discovered via the reader's optimized generator,
        which fetches metadata (titles, URLs, timestamps) in single API requests.

        Yields:
            IngestionItem objects containing page metadata for processing
        """
        logger.info(f"Starting to list pages from {self._reader.api_url}")

        for page_record in self._reader._get_all_pages_generator():
            title = page_record["title"]
            yield IngestionItem(
                id=f"mediawiki:{title}",
                source_ref=title,
                last_modified=page_record.get("last_modified"),
                url=page_record.get("url"),
            )

    def get_raw_content(self, item: IngestionItem) -> str:
        """Fetch and return the raw text content of a MediaWiki page.

        Args:
            item: IngestionItem containing the page title in source_ref

        Returns:
            str: The raw text content of the page, or empty string if fetch failed
        """
        page_title = item.source_ref

        logger.debug(f"Fetching content for page: {page_title}")
        docs = self._reader.load_resource(
            page_title, resource_url=item.url, last_modified=item.last_modified
        )

        if not docs:
            logger.warning(f"Failed to fetch content for page: {page_title}")
            return ""

        return docs[0].text

    def get_item_name(self, item: IngestionItem) -> str:
        """Generate a filesystem-safe filename from the MediaWiki page title.

        Args:
            item: IngestionItem containing the page title in source_ref

        Returns:
            Sanitized filename safe for filesystem storage (255 char limit)
        """
        page_title = item.source_ref

        # Replace problematic characters with underscores
        safe_name = re.sub(r'[^\w\-_\.]', '_', page_title)

        # Ensure it doesn't start/end with underscore and limit length
        safe_name = safe_name.strip('_')
        safe_name = safe_name[:255]

        # Handle edge case where sanitization results in empty string
        if not safe_name:
            # Use hash of original title as fallback to ensure uniqueness
            safe_name = f"page_{hashlib.md5(page_title.encode('utf-8')).hexdigest()[:8]}"

        return safe_name

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Provide MediaWiki-specific page URL as extra metadata.

        Args:
            item: IngestionItem containing cached page URL
            content: Raw page content
            metadata: Standard metadata dictionary

        Returns:
            dict: Dictionary with 'url' field if available
        """
        extra = {}
        if item.url:
            extra["url"] = item.url
        else:
            logger.warning(f"URL not found for page: {item.source_ref}")

        return extra
