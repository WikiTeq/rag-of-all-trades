import hashlib
import logging
import re
import time
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from llama_index.readers.web import BeautifulSoupWebReader
from llama_index.readers.web.sitemap.base import SitemapReader

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
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
      - ``urls``: scrape a fixed list of URLs using BeautifulSoupWebReader.
        Supports ``depth`` for BFS link traversal.
      - ``sitemap_url``: discover URLs from a sitemap.xml and scrape them
        using SitemapReader. Supports ``include_prefix`` for URL filtering.

    Configuration (config.yaml):
        - config.urls: list of URLs to scrape (mutually exclusive with sitemap_url)
        - config.sitemap_url: URL of a sitemap.xml (mutually exclusive with urls)
        - config.include_prefix: only include sitemap URLs containing this string
          (only for sitemap_url mode)
        - config.html_to_text: convert HTML to plain text (default True)
        - config.depth: BFS levels to follow from seed URLs (default 0, urls mode only)
        - config.same_domain_only: restrict crawl to seed hosts (default True)
        - config.max_pages: hard cap on total pages crawled (optional)
    """

    @property
    def source_type(self) -> str:
        return "web"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        raw_urls = cfg.get("urls") or []
        if isinstance(raw_urls, str):
            self.urls: list[str] = [u.strip() for u in raw_urls.split(",") if u.strip()]
        else:
            self.urls = [u.strip() for u in raw_urls if u and u.strip()]
        self.sitemap_url: str | None = cfg.get("sitemap_url", "").strip() or None

        if not self.urls and not self.sitemap_url:
            raise ValueError("Either 'urls' or 'sitemap_url' must be set in web connector config")
        if self.urls and self.sitemap_url:
            raise ValueError("'urls' and 'sitemap_url' are mutually exclusive in web connector config")

        self.include_prefix: str | None = cfg.get("include_prefix", "").strip() or None
        self.html_to_text: bool = bool(cfg.get("html_to_text", True))

        self.depth: int = int(cfg.get("depth", 0))
        _sdonly = cfg.get("same_domain_only", True)
        self.same_domain_only: bool = str(_sdonly).lower() not in ("false", "0", "no")
        _max = cfg.get("max_pages")
        if _max is not None:
            _max = int(_max)
            if _max <= 0:
                raise ValueError("max_pages must be a positive integer")
        self.max_pages: int | None = _max

        if self.depth > 0 and self.sitemap_url:
            raise ValueError("depth > 0 is not supported with sitemap_url mode; sitemap already enumerates pages")

        self.website_extractor = CatchAllWebsiteExtractor(_title_extractor)
        self._reader = BeautifulSoupWebReader(website_extractor=self.website_extractor)

        if self.sitemap_url:
            logger.info(
                f"Initialized Web connector (mode=sitemap, url={self.sitemap_url!r}, "
                f"include_prefix={self.include_prefix!r}, html_to_text={self.html_to_text})"
            )
        else:
            logger.info(
                f"Initialized Web connector (mode=urls, count={len(self.urls)}, "
                f"depth={self.depth}, same_domain_only={self.same_domain_only}, "
                f"max_pages={self.max_pages}, html_to_text={self.html_to_text})"
            )

    def list_items(self) -> Iterator[IngestionItem]:
        """Yield one IngestionItem per discovered URL."""
        logger.info(f"[{self.source_name}] Discovering web pages")

        if self.sitemap_url:
            urls = self._discover_sitemap_urls()
        elif self.depth > 0:
            self._crawl_cache: dict[str, dict] = {}
            urls = self._crawl(self.urls)
        else:
            urls = self.urls

        logger.info(f"[{self.source_name}] Found {len(urls)} URL(s)")

        for url in urls:
            item = IngestionItem(
                id=f"web:{url}",
                source_ref=url,
                last_modified=datetime.now(UTC),
            )
            if self.depth > 0:
                item._metadata_cache.update(self._crawl_cache.get(url, {}))
            yield item

    def get_raw_content(self, item: IngestionItem) -> str:
        """Fetch and return page content as text."""
        url: str = item.source_ref
        item._metadata_cache["url"] = url

        if "_crawl_text" in item._metadata_cache:
            item._metadata_cache.setdefault("title", item._metadata_cache.get("_crawl_title", "") or url)
            return item._metadata_cache["_crawl_text"]

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
        safe = re.sub(r"[^\w\-]", "_", url)
        suffix = hashlib.sha1(url.encode()).hexdigest()[:8]
        return f"{safe[:246]}-{suffix}"

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        """Return web-specific metadata fields."""
        return {
            "url": item._metadata_cache.get("url", ""),
            "title": item._metadata_cache.get("title", ""),
        }

    def _crawl(self, seed_urls: list[str]) -> list[str]:
        """Return all URLs reachable from seed_urls within self.depth hops."""

        def _canon(url: str) -> str:
            # Strip fragment and trailing slash so the same page isn't visited twice
            # under slightly different forms (e.g. /page vs /page/).
            return url.split("#")[0].rstrip("/") or url

        # Allowed hosts are derived from seeds, not the current page being crawled.
        # This means a redirect to an external domain is still blocked when same_domain_only=True.
        seed_hosts: set[str] = {urlparse(u).netloc for u in seed_urls if u.startswith(("http://", "https://"))}

        # dict.fromkeys preserves insertion order and gives O(1) membership checks.
        # Seeds are pre-loaded so they're never re-queued even if found as links later.
        visited: dict[str, None] = dict.fromkeys(_canon(u) for u in seed_urls)
        frontier: list[str] = list(visited)

        headers = {"User-Agent": "Mozilla/5.0 (compatible; rag-of-all-trades-bot/1.0)"}

        for _ in range(self.depth):
            next_frontier: list[str] = []
            for url in frontier:
                try:
                    resp = requests.get(url, timeout=10, headers=headers, allow_redirects=True)
                    resp.raise_for_status()
                    ct = resp.headers.get("Content-Type", "")
                    # Only parse HTML; skip PDFs, images, CSS, etc.
                    if "text/html" not in ct:
                        continue

                    soup = BeautifulSoup(resp.text, "html.parser")

                    # Cache extracted text so get_raw_content skips a second HTTP request.
                    text, meta = _title_extractor(soup)
                    self._crawl_cache[url] = {
                        "_crawl_text": text,
                        "_crawl_title": meta.get("title", ""),
                    }

                    # Resolve relative links against <base href> if present, otherwise
                    # against the current page URL.
                    base_tag = soup.find("base", href=True)
                    base_url = urljoin(url, base_tag["href"]) if base_tag else url

                    for tag in soup.find_all("a", href=True):
                        link = _canon(urljoin(base_url, tag["href"]))
                        # Drop non-HTTP schemes: mailto:, javascript:, data:, etc.
                        if not link.startswith(("http://", "https://")):
                            continue
                        if self.same_domain_only and urlparse(link).netloc not in seed_hosts:
                            continue
                        if link not in visited:
                            # Check cap before adding so we never exceed max_pages.
                            if self.max_pages and len(visited) >= self.max_pages:
                                return list(visited)
                            visited[link] = None
                            next_frontier.append(link)
                except Exception as e:
                    logger.warning(f"[{self.source_name}] Link crawl failed for {url}: {e}")

                if self.request_delay:
                    time.sleep(self.request_delay)

            # Advance one BFS level; stop early if no new URLs were discovered.
            frontier = next_frontier
            if not frontier:
                break

        return list(visited)

    def _discover_sitemap_urls(self) -> list[str]:
        """Parse the configured sitemap and return matching URLs."""
        reader = SitemapReader(html_to_text=self.html_to_text)
        # SitemapReader.load_data returns Documents; we need just the URLs.
        # We use _load_sitemap + _parse_sitemap to get URLs without fetching pages.
        raw = reader._load_sitemap(self.sitemap_url)
        urls = reader._parse_sitemap(raw, filter_locs=self.include_prefix)
        return urls
