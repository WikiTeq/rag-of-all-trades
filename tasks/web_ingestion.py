from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime
from typing import Any

from llama_index.readers.web import BeautifulSoupWebReader
from llama_index.readers.web.sitemap.base import SitemapReader

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.config_validation import mutually_exclusive
from utils.logger import logging
from utils.parse import parse_bool, parse_list
from utils.text import slugify

# Configure logging

logger = logging.getLogger(__name__)


Extractor = Callable[..., tuple[str, dict[str, Any]]]


class CatchAllWebsiteExtractor(Mapping[str, Extractor]):
    """Map every hostname to the same default extractor.

    ``BeautifulSoupWebReader`` treats ``website_extractor`` as a hostname->callable
    mapping. This implementation makes the catch-all behavior explicit while still
    allowing future host-specific overrides if we ever need them.
    """

    def __init__(
        self,
        default_extractor: Extractor,
        overrides: dict[str, Extractor] | None = None,
    ):
        """Initialise with a default extractor and optional per-host overrides."""
        self._default = default_extractor
        self._overrides = overrides or {}

    def __bool__(self):
        """Always truthy so BeautifulSoupWebReader treats it as a non-empty mapping."""
        return True

    def __contains__(self, key):
        """Return True for any string key, making every hostname a match."""
        return isinstance(key, str)

    def __getitem__(self, key):
        """Return the host-specific extractor if present, otherwise the default."""
        return self._overrides.get(key, self._default)

    def __iter__(self):
        """Iterate over explicitly overridden hostnames only."""
        return iter(self._overrides)

    def __len__(self):
        """Return the number of explicit hostname overrides."""
        return len(self._overrides)


def _title_extractor(soup, **_):
    """Extract plain text and page title from a BeautifulSoup object."""
    title_tag = soup.find("title")
    title = title_tag.getText().strip() if title_tag else ""
    return soup.getText(), {"title": title}


class WebIngestionJob(IngestionJob):
    """Ingestion connector for web pages.

    Supports two modes (mutually exclusive):
      - ``urls``: scrape a fixed list of URLs using BeautifulSoupWebReader
      - ``sitemap_url``: discover URLs from a sitemap.xml and scrape them
        using SitemapReader. Supports ``include_prefix`` for URL filtering.

    Configuration (config.yaml):
        - config.urls: list of URLs to scrape (mutually exclusive with sitemap_url)
        - config.sitemap_url: URL of a sitemap.xml (mutually exclusive with urls)
        - config.include_prefix: only include sitemap URLs containing this string
          (only for sitemap_url mode)
        - config.html_to_text: convert HTML to plain text (default True)
    """

    @property
    def source_type(self) -> str:
        return "web"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        self.urls = parse_list(cfg.get("urls"))
        self.sitemap_url: str | None = cfg.get("sitemap_url", "").strip() or None

        if not self.urls and not self.sitemap_url:
            raise ValueError("Either 'urls' or 'sitemap_url' must be set in web connector config")
        mutually_exclusive(cfg, "urls", "sitemap_url", "web connector")

        self.include_prefix: str | None = cfg.get("include_prefix", "").strip() or None
        self.html_to_text = parse_bool(cfg.get("html_to_text"), default=True)

        self.website_extractor = CatchAllWebsiteExtractor(_title_extractor)
        self._reader = BeautifulSoupWebReader(website_extractor=self.website_extractor)

        if self.sitemap_url:
            logger.info(
                f"Initialized Web connector (mode=sitemap, url={self.sitemap_url!r}, "
                f"include_prefix={self.include_prefix!r}, html_to_text={self.html_to_text})"
            )
        else:
            logger.info(
                f"Initialized Web connector (mode=urls, count={len(self.urls)}, html_to_text={self.html_to_text})"
            )

    def list_items(self) -> Iterator[IngestionItem]:
        """Yield one IngestionItem per discovered URL."""
        logger.info(f"[{self.source_name}] Discovering web pages")

        if self.sitemap_url:
            urls = self._discover_sitemap_urls()
        else:
            urls = self.urls

        logger.info(f"[{self.source_name}] Found {len(urls)} URL(s)")

        for url in urls:
            yield IngestionItem(
                id=f"web:{url}",
                source_ref=url,
                last_modified=datetime.now(UTC),
            )

    def get_raw_content(self, item: IngestionItem) -> str:
        """Fetch and return page content as text."""
        url: str = item.source_ref
        item._metadata_cache["url"] = url

        try:
            docs = self._reader.load_data(urls=[url])
            if not docs:
                return ""

            item._metadata_cache["title"] = docs[0].metadata.get("title", "") or url

            return docs[0].text or ""
        except Exception as e:
            logger.warning(f"[{self.source_name}] Failed to fetch {url}: {e}")
            return ""

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe name derived from the URL."""
        url: str = item.source_ref
        return slugify(url, max_len=255)

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        """Return web-specific metadata fields."""
        return {
            "url": item._metadata_cache.get("url", ""),
            "title": item._metadata_cache.get("title", ""),
        }

    def _discover_sitemap_urls(self) -> list[str]:
        """Parse the configured sitemap and return matching URLs."""
        reader = SitemapReader(html_to_text=self.html_to_text)
        # SitemapReader.load_data returns Documents; we need just the URLs.
        # We use _load_sitemap + _parse_sitemap to get URLs without fetching pages.
        raw = reader._load_sitemap(self.sitemap_url)
        urls = reader._parse_sitemap(raw, filter_locs=self.include_prefix)
        return urls
