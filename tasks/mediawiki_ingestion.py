# Standard library imports
import hashlib
import logging
import re
from collections.abc import Iterator
from typing import Any

# Third-party imports
from llama_index.readers.mediawiki import MediaWikiReader

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

# Configure logging
# TODO: Logging should not be done here and in s3, but in the main module
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class MediaWikiIngestionJob(IngestionJob):
    @property
    def source_type(self) -> str:
        return "mediawiki"

    def __init__(self, config):
        """Initialize the MediaWiki ingestion job.

        Args:
            config: Configuration dictionary containing:
                - config.host: MediaWiki site hostname, e.g. 'wiki.example.org' (required)
                - config.path: MediaWiki script path (optional, default '/w/')
                - config.scheme: URL scheme 'https' or 'http' (optional, default 'https')
                - config.page_limit: Max page titles per allpages API call (optional, default 500)
                - config.namespaces: List of namespace IDs to include (optional, None = content namespaces)
                - config.filter_redirects: Exclude redirect pages (optional, default True)
                - config.username: MediaWiki username or bot username (optional, for private wikis)
                - config.password: MediaWiki password or bot password (optional, for private wikis)

        Raises:
            ValueError: If host is not provided
        """
        super().__init__(config)

        cfg = config.get("config", {})

        host = cfg.get("host", "").strip()
        if not host:
            raise ValueError("host is required and must be non-empty")

        raw = cfg.get("namespaces")
        if isinstance(raw, str):
            namespaces = [int(n.strip()) for n in raw.split(",") if n.strip()]
        elif isinstance(raw, int):
            namespaces = [raw]
        else:
            namespaces = raw

        self._reader = MediaWikiReader(
            host=host,
            path=cfg.get("path", "/w/"),
            scheme=cfg.get("scheme", "https"),
            page_limit=cfg.get("page_limit"),
            namespaces=namespaces,
            filter_redirects=cfg.get("filter_redirects", True),
        )

        username = cfg.get("username")
        password = cfg.get("password")
        if username and password:
            self._reader.login(username, password)

        logger.info(
            "Initialized MediaWiki connector for %s://%s%s",
            self._reader.scheme,
            self._reader.host,
            self._reader.path,
        )

    def list_items(self) -> Iterator[IngestionItem]:
        """Discover all pages in the MediaWiki instance and yield ingestion items.

        Iterates through pages discovered via the reader's _get_all_pages_generator,
        which uses mwclient's allpages API to fetch titles, URLs, timestamps,
        page IDs, and namespace IDs in a single streaming pass.

        Yields:
            IngestionItem objects containing page metadata for processing
        """
        base_url = f"{self._reader.scheme}://{self._reader.host}{self._reader.path}"
        logger.info(f"Starting to list pages from {base_url}")

        for page_record in self._reader._get_all_pages_generator():
            title = page_record.title
            yield IngestionItem(
                id=f"mediawiki:{title}",
                source_ref=page_record,
                last_modified=page_record.last_modified,
            )

    def get_raw_content(self, item: IngestionItem) -> str:
        """Fetch and return the raw text content of a MediaWiki page.

        Args:
            item: IngestionItem with page_record in source_ref

        Returns:
            str: The raw text content of the page, or empty string if fetch failed
        """
        page_record = item.source_ref

        logger.debug(f"Fetching content for page: {page_record.title}")
        doc = self._reader._page_to_document(page_record)

        if doc is None:
            logger.warning(f"Failed to fetch content for page: {page_record.title}")
            return ""

        return doc.text

    def get_item_name(self, item: IngestionItem) -> str:
        """Generate a filesystem-safe, unique filename from the MediaWiki page title.

        Namespace separators are normalized so "Page/One" and "Page:One" do not
        collide: ':' -> '__', '/' -> '_', then other non-word chars -> '_'.

        Args:
            item: IngestionItem with page_record in source_ref

        Returns:
            Sanitized filename safe for filesystem storage (255 char limit)
        """
        page_title = item.source_ref.title

        # Preserve namespace distinction: : -> __, / -> _, then other bad chars -> _
        safe_name = page_title.replace(":", "__").replace("/", "_")
        safe_name = re.sub(r"[^\w\-_.]", "_", safe_name)

        # Ensure it doesn't start/end with underscore and limit length
        safe_name = safe_name.strip("_")[:255]

        # Fallback for empty after sanitization (MD5 used for uniqueness only, not crypto)
        if not safe_name:
            safe_name = f"page_{hashlib.md5(page_title.encode('utf-8')).hexdigest()[:8]}"

        return safe_name

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        """Provide MediaWiki-specific metadata for the page.

        Args:
            item: IngestionItem with page_record in source_ref
            content: Raw page content (unused)
            metadata: Standard metadata dictionary (do not return keys that overlap with it)

        Returns:
            dict: Additional metadata (title, url, page_id, namespace)
        """
        page_record = item.source_ref
        extra: dict[str, Any] = {
            "title": page_record.title,
            "page_id": page_record.pageid,
            "namespace": page_record.namespace,
        }
        if page_record.url:
            extra["url"] = page_record.url
        else:
            logger.warning(f"URL not found for page: {page_record.title}")
        return extra
