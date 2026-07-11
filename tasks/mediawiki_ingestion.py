import json
import logging
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlsplit

from llama_index.readers.mediawiki import MediaWikiReader

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.parse import parse_bool
from utils.text import slugify

logger = logging.getLogger(__name__)


class MediaWikiIngestionJob(IngestionJob):
    @property
    def source_type(self) -> str:
        return "mediawiki"

    def __init__(self, config):
        """Initialize the MediaWiki ingestion job.

        Args:
            config: Configuration dictionary containing:
                - config.api_url: URL to the Mediawiki API (mutually exclusive with the host,path,scheme)
                - config.host: MediaWiki site hostname, e.g. 'wiki.example.org' (required)
                - config.path: MediaWiki script path (optional, default '/w/')
                - config.scheme: URL scheme 'https' or 'http' (optional, default 'https')
                - config.page_limit: Max page titles per allpages API call (optional, default 500)
                - config.namespaces: List of namespace IDs to include (optional, None = content namespaces)
                - config.filter_redirects: Exclude redirect pages (optional, default True)
                - config.username: MediaWiki username or bot username (optional, for private wikis)
                - config.password: MediaWiki password or bot password (optional, for private wikis)
                - config.load_semantics: Query Semantic MediaWiki properties per page and attach
                  them as metadata (optional, default False)

        Raises:
            ValueError: If host is not provided
        """
        super().__init__(config)

        cfg = config.get("config", {})

        api_url = cfg.get("api_url", "").strip()
        host = cfg.get("host", "").strip()

        if not host and not api_url:
            raise ValueError("Either host or api_url is required in MediaWiki connector config")

        if host and api_url:
            raise ValueError("Only one of host/scheme/path or api_url can be provided in MediaWiki connector config")

        if api_url:
            api_url_parsed = urlsplit(api_url, allow_fragments=False)
            if not api_url_parsed.scheme or not api_url_parsed.hostname:
                raise ValueError("Invalid api_url in MediaWiki connector config")
            host = api_url_parsed.hostname
            path = api_url_parsed.path.strip().removesuffix("api.php")
            scheme = api_url_parsed.scheme
        else:
            if not host:
                raise ValueError("host is required and must be non-empty")
            path = cfg.get("path", "/w/").strip()
            scheme = cfg.get("scheme", "https").strip()

        raw = cfg.get("namespaces")
        if isinstance(raw, str):
            namespaces = [int(n.strip()) for n in raw.split(",") if n.strip()]
        elif isinstance(raw, int):
            namespaces = [raw]
        else:
            namespaces = raw

        self._reader = MediaWikiReader(
            host=host,
            path=path,
            scheme=scheme,
            page_limit=cfg.get("page_limit"),
            namespaces=namespaces,
            filter_redirects=cfg.get("filter_redirects", True),
            logger=logger,
        )

        username = cfg.get("username")
        password = cfg.get("password")
        if username and password:
            self._reader.login(username, password)

        self.load_semantics = parse_bool(cfg.get("load_semantics"))

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

    def get_item_checksum(self, item: IngestionItem) -> str | None:
        """Return the page's lastrevid as a checksum string.

        item.source_ref is a Page dataclass whose revision field holds
        MediaWiki's lastrevid — a globally incrementing integer incremented on
        every page edit. Using it avoids fetching full page content just to
        detect whether a page has changed.

        Returns None when revision is 0 or absent, falling back to content-based MD5.
        """
        revision = item.source_ref.revision
        if revision:
            return str(revision)
        return None

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
        return slugify(page_title, max_len=255, extra_replacements={":": "__", "/": "_"})

    # SMW\DataItem::TYPE_WIKIPAGE — the default type for untyped SMW properties.
    _SMW_TYPE_WIKIPAGE = 9

    @classmethod
    def _decode_smw_dataitem_value(cls, dataitem: dict[str, Any]) -> str:
        """Decode a single smwbrowse dataitem entry into a display string.

        Wikipage-type values (SMW's default type for untyped properties) are serialized
        as "DBkey#namespace#interwiki#subobjectname" (see SMW's DIWikiPage::getSerialization);
        only the DBkey segment is human-readable and it uses underscores in place of spaces.
        Other types (text, number, time, ...) are already plain values.
        """
        item = str(dataitem.get("item", ""))
        if dataitem.get("type") == cls._SMW_TYPE_WIKIPAGE:
            return item.split("#", 1)[0].replace("_", " ")
        return item

    # Prefix for semantic property metadata keys, so they stay flat (filterable via the
    # existing /api/v1/query metadata filter API, which only matches top-level keys) while
    # never colliding with connector-owned keys (title, page_id, namespace, url).
    _SMW_METADATA_PREFIX = "smw_"

    def _load_semantic_properties(self, title: str, namespace: int) -> dict[str, str]:
        """Query Semantic MediaWiki for a page's properties via the smwbrowse API action.

        Excludes system properties (leading underscore, e.g. _ASK, _INST, _SKEY) and
        subobjects (sobj). Only the first value is kept for multi-valued properties.
        Each property is returned under a "smw_"-prefixed key to avoid colliding with
        connector-owned metadata keys.

        Returns an empty dict if the query fails for any reason (SMW not installed,
        page has no semantic data, transient error) — ingestion of the page continues
        without semantic metadata rather than failing the item.
        """
        try:
            params = json.dumps({"subject": title, "ns": namespace, "iw": "", "subobject": ""})
            response = self._reader.site.get("smwbrowse", browse="subject", params=params, format="json")
        except Exception:
            logger.warning(f"Failed to fetch semantic properties for page: {title}")
            return {}

        properties: dict[str, str] = {}
        for entry in response.get("query", {}).get("data", []):
            property_key = entry.get("property", "")
            if not property_key or property_key.startswith("_"):
                continue
            dataitems = entry.get("dataitem", [])
            if dataitems:
                properties[self._SMW_METADATA_PREFIX + property_key] = self._decode_smw_dataitem_value(dataitems[0])
        return properties

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        """Provide MediaWiki-specific metadata for the page.

        Args:
            item: IngestionItem with page_record in source_ref
            content: Raw page content (unused)
            metadata: Standard metadata dictionary (do not return keys that overlap with it)

        Returns:
            dict: Additional metadata (title, url, page_id, namespace, and semantic
                properties when config.load_semantics is enabled)
        """
        page_record = item.source_ref
        extra: dict[str, Any] = {}
        if self.load_semantics:
            extra.update(self._load_semantic_properties(page_record.title, page_record.namespace))
        extra["title"] = page_record.title
        extra["page_id"] = page_record.pageid
        extra["namespace"] = page_record.namespace
        if page_record.url:
            extra["url"] = page_record.url
        else:
            logger.warning(f"URL not found for page: {page_record.title}")
        return extra
