# Standard library imports
import gc
import hashlib
import logging
import re
import time
from datetime import datetime
from typing import Optional, Dict, Any, Iterator

# Third-party imports
import requests
import html2text

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

# Configure logging
# TODO: Logging should not be done here and in s3, but in the main module
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
        """Initialize the MediaWiki ingestion job with API configuration.

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

        # MediaWiki API configuration - require explicit URLs
        self.api_url = cfg.get("api_url")
        if not self.api_url:
            raise ValueError("api_url is required in MediaWiki connector config")

        # Request configuration
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': cfg.get('user_agent', 'rag-of-all-trades-connector-mediawiki/1.0')
        })

        # Rate limiting and performance settings
        self.request_delay = cfg.get('request_delay', 0.1)  # seconds between requests
        if self.request_delay < 0:
            raise ValueError("request_delay must be non-negative")

        self.page_limit = cfg.get('page_limit', 500)  # pages per API call
        if self.page_limit <= 0:
            raise ValueError("page_limit must be positive")

        self.batch_size = cfg.get('batch_size', 50)  # pages to process timestamps for at once
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")

        self.max_retries = cfg.get('max_retries', 3)  # max retries for API requests
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")

        self.timeout = cfg.get('timeout', 30)  # HTTP request timeout in seconds
        if self.timeout <= 0:
            raise ValueError("timeout must be positive")

        # Content filtering
        self.namespaces = cfg.get('namespaces')  # List of namespace IDs to include (None = all)

        logger.info(f"Initialized MediaWiki connector for {self.api_url}")

    def close(self):
        """Close the HTTP session to free up resources."""
        if hasattr(self, 'session'):
            self.session.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures session is closed."""
        self.close()

    def _make_api_request(self, params: Dict[str, Any], max_retries: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Make a request to the MediaWiki API with comprehensive error handling and retries.

        Args:
            params: Dictionary of API parameters (format=json will be added automatically)
            max_retries: Maximum number of retry attempts (default from config)

        Returns:
            Parsed JSON response data, or None if all retries failed

        Handles rate limiting (429 responses), network errors, and exponential backoff.
        """
        if max_retries is None:
            max_retries = self.max_retries

        if max_retries <= 0:
            return None

        for attempt in range(max_retries):
            try:
                # Ensure JSON format for all MediaWiki API responses
                params['format'] = 'json'

                response = self.session.get(self.api_url, params=params, timeout=self.timeout)

                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 5))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                logger.warning(f"API request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"API request failed after {max_retries} attempts")
                    return None

            except ValueError as e:
                logger.error(f"Invalid JSON response: {e}")
                return None

    def _get_all_pages(self) -> Iterator[Dict[str, Any]]:
        """Generator that yields all pages from the MediaWiki instance using pagination.

        Uses the MediaWiki API's allpages query with continuation tokens to handle
        large wikis that require multiple requests. Applies rate limiting between requests.

        Yields:
            Dictionary containing page metadata (title, etc.) for each page
        """
        continue_params = {}

        while True:
            params = {
                'action': 'query',
                'list': 'allpages',
                'aplimit': self.page_limit,
                **continue_params
            }

            # Filter by namespaces if specified (0 = main namespace, 1 = talk, etc.)
            if self.namespaces is not None:
                params['apnamespace'] = [str(ns) for ns in self.namespaces]

            data = self._make_api_request(params)
            if not data:
                break

            pages = data.get('query', {}).get('allpages', [])
            for page in pages:
                yield page

            # Check for continuation
            continue_info = data.get('continue')
            if continue_info:
                continue_params = continue_info
                time.sleep(self.request_delay)  # Rate limiting
            else:
                break

    def _get_page_data(self, page_title: str, **api_params) -> Optional[Dict[str, Any]]:
        """Helper method for making MediaWiki page queries with common response handling.

        Args:
            page_title: The title of the page to query
            **api_params: Additional API parameters (prop, rvprop, inprop, etc.)

        Returns:
            Page data dictionary from the API response, or None if page doesn't exist or request failed
        """
        params = {
            'action': 'query',
            'titles': page_title,
            **api_params
        }

        data = self._make_api_request(params)
        if not data:
            return None

        pages = data.get('query', {}).get('pages', {})
        if not pages:
            return None

        # Get the first (and should be only) page
        page_data = next(iter(pages.values()))

        # Check if page exists (missing pages have pageid = -1 and missing = True)
        if page_data.get('pageid') == -1 or page_data.get('missing') == True:
            logger.warning(f"Page '{page_title}' is missing")
            return None

        return page_data

    def _get_page_info(self, page_title: str) -> Optional[tuple[str, str]]:
        """Fetch the parsed content and canonical URL of a specific MediaWiki page.

        Fetches rendered HTML content instead of raw wikitext because:
        - Raw wikitext contains {{templates}} and [[links]] meaningless to LLMs
        - Parsed content provides expanded templates and resolved links
        - Results in cleaner, more readable text for LLM consumption

        Args:
            page_title: The title of the page to fetch

        Returns:
            A tuple of (parsed_content, canonical_url), or None if page doesn't exist or fetch failed
        """
        # First get the canonical URL using the info API
        url_data = self._get_page_data(
            page_title,
            prop='info',
            inprop='url'
        )
        if not url_data:
            return None

        canonical_url = url_data.get('canonicalurl')
        if not canonical_url:
            logger.warning(f"No URL found for page '{page_title}'")
            return None

        # Now get the parsed content
        params = {
            'action': 'parse',
            'page': page_title,
            'prop': 'text',
            'disableeditsection': 'true',
            'disabletoc': 'true',
            'disablelimitreport': 'true',
            'format': 'json'
        }

        parsed_data = self._make_api_request(params)
        if not parsed_data:
            return None

        parse_result = parsed_data.get('parse', {})
        if not parse_result:
            logger.warning(f"No parse result for page '{page_title}'")
            return None

        html_content = parse_result.get('text', {}).get('*', '')
        if not html_content:
            logger.warning(f"No content in parse result for page '{page_title}'")
            return None

        # Convert HTML to clean text
        clean_content = self._html_to_clean_text(html_content)

        return clean_content, canonical_url

    def _get_page_url(self, page_title: str) -> Optional[str]:
        """Get the canonical URL for a MediaWiki page.

        This is a convenience method that extracts just the URL from _get_page_info.
        Primarily used for testing URL retrieval functionality.
        """
        page_info = self._get_page_info(page_title)
        return page_info[1] if page_info else None

    def _html_to_clean_text(self, html_content: str) -> str:
        """Convert MediaWiki HTML content to clean, readable text for LLMs.

        Uses html2text to convert HTML to Markdown while preserving structure
        and removing unwanted elements like scripts and styles.

        Args:
            html_content: Raw HTML from MediaWiki parse API

        Returns:
            Clean text in Markdown format with HTML tags removed
        """
        try:
            # Configure html2text for clean MediaWiki content extraction
            h = html2text.HTML2Text()
            h.ignore_links = True          # Remove link URLs, keep link text
            h.ignore_images = True         # Remove image references
            h.body_width = 0               # No line wrapping
            h.ul_item_mark = '-'           # Use dashes for unordered lists
            h.emphasis_mark = '*'          # Use * for emphasis instead of _
            h.strong_mark = '**'           # Use ** for strong emphasis

            # Convert HTML to clean Markdown
            result = h.handle(html_content).strip()

            return result

        except Exception as e:
            logger.error(f"html2text conversion failed: {e}")
            # Fallback to basic text extraction
            clean_text = re.sub(r'<[^>]+>', '', html_content)
            clean_text = re.sub(r'\s+', ' ', clean_text).strip()
            return clean_text


    def _get_pages_last_modified(self, page_titles: list[str]) -> Dict[str, Optional[datetime]]:
        """Get last modified timestamps for multiple MediaWiki pages.

        Args:
            page_titles: List of page titles to check

        Returns:
            Dictionary mapping page titles to datetime objects or None

        This method batches multiple page queries into a single API request to avoid N+1 queries.
        """
        if not page_titles:
            return {}

        # MediaWiki API can handle multiple titles in one query, separated by |
        titles_param = '|'.join(page_titles)

        params = {
            'action': 'query',
            'titles': titles_param,
            'prop': 'revisions',
            'rvprop': 'timestamp'
        }

        data = self._make_api_request(params)
        if not data:
            # Return None for all pages if request failed
            return {title: None for title in page_titles}

        pages = data.get('query', {}).get('pages', {})

        # Build title -> page data mapping for efficient lookup
        title_to_page = {page_info.get('title'): page_info for page_info in pages.values()}

        result = {}
        for title in page_titles:
            page_data = title_to_page.get(title)

            if not page_data or 'pageid' not in page_data:
                # Page doesn't exist or is missing
                result[title] = None
                continue

            revisions = page_data.get('revisions', [])
            if revisions:
                timestamp_str = revisions[0].get('timestamp')
                if timestamp_str:
                    try:
                        # MediaWiki timestamps are in ISO 8601 format and parse directly
                        result[title] = datetime.fromisoformat(timestamp_str)
                    except ValueError as e:
                        logger.warning(f"Failed to parse timestamp '{timestamp_str}' for page '{title}': {e}")
                        result[title] = None
                else:
                    result[title] = None
            else:
                result[title] = None

        return result


    def list_items(self) -> Iterator[IngestionItem]:
        """Discover all pages in the MediaWiki instance and yield ingestion items.

        Iterates through pages discovered via the API query, fetching last modified
        timestamps in batches to avoid N+1 query problems. Applies rate limiting
        between batch requests.

        Yields:
            IngestionItem objects containing page metadata for processing
        """
        logger.info(f"Starting to list pages from {self.api_url}")

        # Batch size for fetching timestamps (MediaWiki API limits apply)
        page_batch = []

        for page in self._get_all_pages():
            page_title = page.get('title')
            if not page_title:
                continue

            page_batch.append(page_title)

            # Process batch when it reaches the limit
            if len(page_batch) >= self.batch_size:
                yield from self._process_page_batch(page_batch)
                page_batch = []

        # Process remaining pages in final batch
        if page_batch:
            yield from self._process_page_batch(page_batch)

    def _process_page_batch(self, page_titles: list[str]) -> Iterator[IngestionItem]:
        """Process a batch of page titles, fetching timestamps in bulk and yielding IngestionItems.

        Args:
            page_titles: List of page titles to process

        Yields:
            IngestionItem objects for each page in the batch
        """
        # Get timestamps for all pages in this batch
        timestamps = self._get_pages_last_modified(page_titles)

        for page_title in page_titles:
            last_modified = timestamps.get(page_title)

            yield IngestionItem(
                id=f"mediawiki:{page_title}",
                source_ref=page_title,
                last_modified=last_modified
            )

        # Rate limiting between batch requests
        time.sleep(self.request_delay)

    def get_raw_content(self, item: IngestionItem) -> str:
        """Fetch and return the raw text content of a MediaWiki page.

        Args:
            item: IngestionItem containing the page title in source_ref

        Returns:
            str: The raw text content of the page, or empty string if fetch failed
        """
        page_title = item.source_ref

        logger.debug(f"Fetching content for page: {page_title}")
        page_info = self._get_page_info(page_title)

        if page_info is None:
            logger.warning(f"Failed to fetch content for page: {page_title}")
            return ""

        content, url = page_info
        # Cache URL for use in get_document_metadata()
        item._metadata_cache['page_url'] = url
        return content

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

    def process_item(self, item: IngestionItem):
        """Process a single ingestion item with rate limiting.

        Overrides base implementation to add rate limiting between page fetches.
        The URL is automatically cached by get_raw_content() for use in get_document_metadata().

        Args:
            item: The ingestion item to process

        Returns:
            int: 1 if item was successfully ingested, 0 if skipped or failed
        """
        # Rate limiting between individual page fetches
        time.sleep(self.request_delay)

        # Delegate to base class implementation
        # get_raw_content() will cache the URL in item._metadata_cache
        return super().process_item(item)

    def get_document_metadata(self, item: IngestionItem, item_name: str, checksum: str, version: int, last_modified: datetime) -> Dict[str, Any]:
        """Generate document metadata with MediaWiki-specific page URL.

        Uses explicitly cached URL from process_item() to avoid coupling to base class internals.

        Args:
            item: IngestionItem containing cached page URL
            item_name: Generated filename for the document
            checksum: MD5 hash of content for duplicate detection
            version: Version number of this content
            last_modified: Timestamp of last page modification

        Returns:
            Dictionary with standard metadata plus 'url' field from MediaWiki API
        """
        # Get base metadata
        metadata = super().get_document_metadata(item, item_name, checksum, version, last_modified)

        # Get URL from explicitly cached data
        page_url = item._metadata_cache.get('page_url')
        if page_url:
            metadata["url"] = page_url
        else:
            logger.warning(f"URL not cached for page: {item.source_ref} - this should not happen")

        return metadata
