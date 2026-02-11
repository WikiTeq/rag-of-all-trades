# Standard library imports
import hashlib
import logging
import re
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

        # Validate required and numeric fields (reader also validates; this keeps job contract clear)
        api_url = cfg.get("api_url", "").strip()
        if not api_url:
            raise ValueError("api_url is required and must be non-empty")

        def _check_positive(name: str, default: Any) -> float:
            val = cfg.get(name, default)
            try:
                f = float(val)
            except (TypeError, ValueError):
                raise ValueError(f"{name} must be a number, got {val!r}")
            return f

        def _check_positive_int(name: str, default: int) -> int:
            val = cfg.get(name, default)
            try:
                i = int(val)
            except (TypeError, ValueError):
                raise ValueError(f"{name} must be an integer, got {val!r}")
            if i <= 0:
                raise ValueError(f"{name} must be > 0, got {i}")
            return i

        request_delay = _check_positive("request_delay", 0.1)
        if request_delay < 0:
            raise ValueError("request_delay must be >= 0")
        page_limit = _check_positive_int("page_limit", 500)
        batch_size = _check_positive_int("batch_size", 50)
        try:
            max_retries = int(cfg.get("max_retries", 3))
        except (TypeError, ValueError):
            raise ValueError(f"max_retries must be an integer, got {cfg.get('max_retries')!r}")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        timeout = _check_positive_int("timeout", 30)

        # Build the reader (schedules are consumed by config/celery, not by the reader)
        self._reader = MediaWikiReader(
            api_url=api_url,
            user_agent=cfg.get("user_agent", "rag-of-all-trades-connector-mediawiki/1.0"),
            request_delay=request_delay,
            page_limit=page_limit,
            batch_size=batch_size,
            max_retries=max_retries,
            timeout=timeout,
            namespaces=cfg.get("namespaces"),
        )

        logger.info(f"Initialized MediaWiki connector for {self._reader.api_url}")

    def list_items(self) -> Iterator[IngestionItem]:
        """Discover all pages in the MediaWiki instance and yield ingestion items.

        Iterates through pages discovered via the reader's optimized generator,
        which fetches metadata (titles, URLs, timestamps) in single API requests.
        If the generator raises mid-iteration, the exception propagates and run()
        will report partial results; the job does not retry mid-stream.

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
        """Generate a filesystem-safe, unique filename from the MediaWiki page title.

        Namespace separators are normalized so "Page/One" and "Page:One" do not
        collide: ':' -> '__', '/' -> '_', then other non-word chars -> '_'.

        Args:
            item: IngestionItem containing the page title in source_ref

        Returns:
            Sanitized filename safe for filesystem storage (255 char limit)
        """
        page_title = item.source_ref

        # Preserve namespace distinction: : -> __, / -> _, then other bad chars -> _
        safe_name = page_title.replace(":", "__").replace("/", "_")
        safe_name = re.sub(r"[^\w\-_.]", "_", safe_name)

        # Ensure it doesn't start/end with underscore and limit length
        safe_name = safe_name.strip("_")[:255]

        # Fallback for empty after sanitization (MD5 used for uniqueness only, not crypto)
        if not safe_name:
            safe_name = f"page_{hashlib.md5(page_title.encode('utf-8')).hexdigest()[:8]}"

        return safe_name

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Provide MediaWiki-specific page title and URL as extra metadata.

        Args:
            item: IngestionItem containing cached page URL
            content: Raw page content
            metadata: Standard metadata dictionary (do not return keys that overlap with it)

        Returns:
            dict: Additional metadata (title, and url if available)
        """
        extra: Dict[str, Any] = {"title": item.source_ref}
        if item.url:
            extra["url"] = item.url
        else:
            logger.warning(f"URL not found for page: {item.source_ref}")
        return extra
