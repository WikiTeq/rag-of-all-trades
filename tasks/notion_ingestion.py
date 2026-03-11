# Standard library imports
import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

# Third-party imports
from llama_index.readers.notion import NotionPageReader

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)

NOTION_SEARCH_URL = "https://api.notion.com/v1/search"


class NotionIngestionJob(IngestionJob):
    """Ingestion connector for Notion workspaces.

    Uses the Notion Search API for page discovery (with trash filtering,
    last_modified, title, and metadata extraction) and the LlamaIndex
    NotionPageReader for content fetching.

    Configuration (config.yaml):
        - config.integration_token: Notion integration token (required)
        - config.page_ids: Comma-separated list of page IDs to ingest (optional)
        - config.database_ids: Comma-separated list of database IDs to ingest (optional)
        - config.request_delay: Seconds to sleep between page reads, default 0 (optional)

    If neither page_ids nor database_ids are provided, all accessible pages and
    databases in the workspace are ingested (load-all mode).
    """

    LOAD_MODE_ALL = "all"
    LOAD_MODE_SELECTIVE = "selective"

    @property
    def source_type(self) -> str:
        return "notion"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        self.integration_token = cfg.get("integration_token", "").strip()
        if not self.integration_token:
            raise ValueError(
                "integration_token is required in Notion connector config"
            )

        self.page_ids: List[str] = self._parse_ids(cfg.get("page_ids", ""))
        self.database_ids: List[str] = self._parse_ids(
            cfg.get("database_ids", "")
        )

        self.request_delay = float(cfg.get("request_delay", 0))
        if self.request_delay < 0:
            raise ValueError("request_delay must be non-negative")

        self._reader = NotionPageReader(
            integration_token=self.integration_token
        )

        load_mode = (
            self.LOAD_MODE_ALL
            if not self.page_ids and not self.database_ids
            else self.LOAD_MODE_SELECTIVE
        )
        logger.info(
            f"Initialized Notion connector "
            f"(mode={load_mode}, page_ids={self.page_ids}, "
            f"database_ids={self.database_ids}, request_delay={self.request_delay})"
        )

    def list_items(self) -> Iterator[IngestionItem]:
        """Discover Notion pages and yield one IngestionItem per page.

        In load-all mode uses the Search API to discover all accessible pages.
        In selective mode resolves configured page_ids and database_ids.
        Trashed pages are always skipped.
        """
        logger.info(f"[{self.source_name}] Discovering Notion pages")

        if not self.page_ids and not self.database_ids:
            yield from self._search_all_pages()
        else:
            yield from self._selective_pages()

    def get_raw_content(self, item: IngestionItem) -> str:
        """Fetch and return the full text content of a Notion page using
        the LlamaIndex reader.

        The reader handles block recursion, retries, and rate limiting.
        An optional request_delay is applied after each page read.
        """
        page_id: str = item.source_ref
        try:
            text = self._reader.read_page(page_id)
        except Exception as e:
            logger.error(
                f"[{self.source_name}] Failed to read page {page_id}: {e}"
            )
            return ""
        finally:
            if self.request_delay > 0:
                time.sleep(self.request_delay)
        return text or ""

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe name derived from the page title, falling back to page ID."""
        title = item._metadata_cache.get("title", "")
        name = title if title else item.source_ref
        safe = re.sub(r"[^\w\-]", "_", name)
        return safe[:255]

    def get_document_metadata(
        self,
        item: IngestionItem,
        item_name: str,
        checksum: str,
        version: int,
        last_modified: Any,
    ) -> Dict[str, Any]:
        """Build metadata dict with Notion-specific fields."""
        page_id: str = item.source_ref
        cache = item._metadata_cache
        metadata = super().get_document_metadata(
            item, item_name, checksum, version, last_modified
        )
        metadata.update(
            {
                "id": page_id,
                "url": cache.get("url") or f"https://notion.so/{page_id.replace('-', '')}",
                "created_time": cache.get("created_time"),
                "parent_type": cache.get("parent_type"),
                "parent_id": cache.get("parent_id"),
            }
        )
        if cache.get("public_url"):
            metadata["public_url"] = cache["public_url"]
        return metadata

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _search_all_pages(self) -> Iterator[IngestionItem]:
        """Use the Notion Search API to yield all non-trashed pages."""
        payload: Dict[str, Any] = {
            "filter": {"value": "page", "property": "object"},
            "page_size": 100,
        }
        total = 0
        while True:
            try:
                resp = self._reader._request_with_retry(
                    "POST", NOTION_SEARCH_URL, headers=self._reader.headers, json=payload
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(
                    f"[{self.source_name}] Search API request failed: {e}"
                )
                break

            for page in data.get("results", []):
                item = self._page_to_item(page)
                if item:
                    total += 1
                    yield item

            if not data.get("has_more"):
                break
            payload["start_cursor"] = data["next_cursor"]

        logger.info(
            f"[{self.source_name}] Total pages discovered via search: {total}"
        )

    def _selective_pages(self) -> Iterator[IngestionItem]:
        """Yield items for explicitly configured page_ids and database_ids."""
        for page_id in self.page_ids:
            try:
                resp = self._reader._request_with_retry(
                    "GET",
                    f"https://api.notion.com/v1/pages/{page_id}",
                    headers=self._reader.headers
                )
                resp.raise_for_status()
                item = self._page_to_item(resp.json())
                if item:
                    yield item
            except Exception as e:
                logger.error(
                    f"[{self.source_name}] Failed to fetch page {page_id}: {e}"
                )

        for db_id in self.database_ids:
            try:
                db_page_ids = self._reader.query_database(db_id)
                logger.info(
                    f"[{self.source_name}] Database {db_id}: found {len(db_page_ids)} page(s)"
                )
                for pid in db_page_ids:
                    try:
                        resp = self._reader._request_with_retry(
                            "GET",
                            f"https://api.notion.com/v1/pages/{pid}",
                            headers=self._reader.headers
                        )
                        resp.raise_for_status()
                        item = self._page_to_item(resp.json())
                        if item:
                            yield item
                    except Exception as e:
                        logger.error(
                            f"[{self.source_name}] Failed to fetch page {pid}: {e}"
                        )
            except Exception as e:
                logger.error(
                    f"[{self.source_name}] Failed to query database {db_id}: {e}"
                )

    def _page_to_item(self, page: Dict[str, Any]) -> Optional[IngestionItem]:
        """Convert a Notion page object to an IngestionItem, or None if trashed."""
        if page.get("in_trash") or page.get("archived"):
            return None

        page_id: str = page.get("id", "")

        last_modified: Optional[datetime] = None
        raw_edited = page.get("last_edited_time")
        if raw_edited:
            try:
                last_modified = datetime.fromisoformat(
                    raw_edited.replace("Z", "+00:00")
                )
            except ValueError:
                pass

        created_time: Optional[datetime] = None
        raw_created = page.get("created_time")
        if raw_created:
            try:
                created_time = datetime.fromisoformat(
                    raw_created.replace("Z", "+00:00")
                )
            except ValueError:
                pass

        title = self._extract_title(page)
        parent = page.get("parent", {})

        item = IngestionItem(
            id=f"notion:{page_id}",
            source_ref=page_id,
            last_modified=last_modified,
        )
        item._metadata_cache.update(
            {
                "title": title,
                "url": page.get("url"),
                "public_url": page.get("public_url"),
                "created_time": created_time,
                "parent_type": parent.get("type"),
                "parent_id": parent.get(parent.get("type")),
            }
        )
        return item

    @staticmethod
    def _extract_title(page: Dict[str, Any]) -> str:
        """Extract the page title from Notion page properties."""
        properties = page.get("properties", {})
        for prop in properties.values():
            if prop.get("type") == "title":
                title_parts = prop.get("title", [])
                return "".join(t.get("plain_text", "") for t in title_parts)
        return ""

    @staticmethod
    def _parse_ids(value: Any) -> List[str]:
        """Parse a comma-separated string or list of IDs into a list of stripped strings."""
        if not value:
            return []
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        return [v.strip() for v in str(value).split(",") if v.strip()]
