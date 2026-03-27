import logging
import re
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from llama_index.readers.web import BeautifulSoupWebReader
from llama_index.readers.web.sitemap.base import SitemapReader

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class _CatchAllExtractorMap(dict):
    """A dict that returns the same extractor for any hostname key.

    BeautifulSoupWebReader looks up extractors via website_extractor.get(hostname).
    This ensures our title extractor is used for every URL regardless of hostname.
    """

    def __init__(self, extractor):
        super().__init__()
        self._extractor = extractor

    def get(self, key, default=None):
        return self._extractor


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

        self.urls: list[str] = cfg.get("urls") or []
        self.sitemap_url: str | None = cfg.get("sitemap_url", "").strip() or None

        if not self.urls and not self.sitemap_url:
            raise ValueError("Either 'urls' or 'sitemap_url' must be set in web connector config")
        if self.urls and self.sitemap_url:
            raise ValueError("'urls' and 'sitemap_url' are mutually exclusive in web connector config")

        self.include_prefix: str | None = cfg.get("include_prefix", "").strip() or None
        self.html_to_text: bool = bool(cfg.get("html_to_text", True))

        self._reader = BeautifulSoupWebReader(website_extractor=_CatchAllExtractorMap(self._title_extractor))

        if self.sitemap_url:
            logger.info(
                f"Initialized Web connector (mode=sitemap, url={self.sitemap_url!r}, "
                f"include_prefix={self.include_prefix!r}, html_to_text={self.html_to_text})"
            )
        else:
            logger.info(
                f"Initialized Web connector (mode=urls, count={len(self.urls)}, html_to_text={self.html_to_text})"
            )

    def _title_extractor(self, soup, **_):
        title_tag = soup.find("title")
        title = title_tag.getText().strip() if title_tag else ""
        return soup.getText(), {"title": title}

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

        docs = self._reader.load_data(urls=[url])
        if not docs:
            return ""
        item._metadata_cache["title"] = docs[0].metadata.get("title", "") or url
        return docs[0].text or ""

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe name derived from the URL."""
        url: str = item.source_ref
        safe = re.sub(r"[^\w\-]", "_", url)
        return safe[:255]

    def get_document_metadata(
        self,
        item: IngestionItem,
        item_name: str,
        checksum: str,
        version: int,
        last_modified: Any,
    ) -> dict[str, Any]:
        metadata = super().get_document_metadata(item, item_name, checksum, version, last_modified)
        metadata.update(
            {
                "url": item._metadata_cache.get("url", ""),
                "title": item._metadata_cache.get("title", ""),
            }
        )
        return metadata

    def _discover_sitemap_urls(self) -> list[str]:
        """Parse the configured sitemap and return matching URLs."""
        reader = SitemapReader(html_to_text=self.html_to_text)
        # SitemapReader.load_data returns Documents; we need just the URLs.
        # We use _load_sitemap + _parse_sitemap to get URLs without fetching pages.
        raw = reader._load_sitemap(self.sitemap_url)
        urls = reader._parse_sitemap(raw, filter_locs=self.include_prefix)
        return urls
