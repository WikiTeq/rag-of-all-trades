# Standard library imports
import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

# Third-party imports
from notion_client import Client
from notion_client.errors import APIResponseError

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)


class NotionIngestionJob(IngestionJob):
    """Ingestion connector for Notion workspaces.

    Uses the official Notion SDK (notion-client 3.0.0) for all API interactions:
    page discovery via Search API, content fetching via Blocks API,
    database querying via DataSources API, and user resolution via Users API.

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
        self._user_cache: Dict[str, Optional[str]] = {}

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

        self._client = Client(auth=self.integration_token)

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
        """Fetch and return the full text content of a Notion page by
        recursively reading all blocks via the Blocks API.

        An optional request_delay is applied after each page read.
        """
        page_id: str = item.source_ref
        try:
            text = self._read_page(page_id)
        except Exception as e:  # _read_page is recursive and may raise unexpected errors, such as KeyError or RecursionError.
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
                "created_by": cache.get("created_by"),
                "last_edited_by": cache.get("last_edited_by"),
            }
        )
        if cache.get("public_url"):
            metadata["public_url"] = cache["public_url"]
        return metadata

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_page(self, page_id: str, num_tabs: int = 0) -> str:
        """Recursively read all text blocks of a Notion page using the Blocks API.

        Fetches children of the given block_id with pagination, extracts
        rich_text content, and recurses into child blocks.
        """
        result_lines = []
        cursor = None

        while True:
            kwargs: Dict[str, Any] = {"block_id": page_id, "page_size": 100}
            if cursor:
                kwargs["start_cursor"] = cursor

            data = self._client.blocks.children.list(**kwargs)

            for block in data.get("results", []):
                block_type = block.get("type", "")
                block_obj = block.get(block_type, {})
                text_parts = []

                if "rich_text" in block_obj:
                    for rich_text in block_obj["rich_text"]:
                        if "text" in rich_text:
                            prefix = "\t" * num_tabs
                            text_parts.append(prefix + rich_text["text"]["content"])

                if block.get("has_children"):
                    children_text = self._read_page(block["id"], num_tabs=num_tabs + 1)
                    text_parts.append(children_text)

                if text_parts:
                    result_lines.append("\n".join(text_parts))

            if not data.get("next_cursor"):
                break
            cursor = data["next_cursor"]

        return "\n".join(result_lines)

    def _search_all_pages(self) -> Iterator[IngestionItem]:
        """Use the Notion Search API to yield all non-trashed pages."""
        kwargs: Dict[str, Any] = {
            "filter": {"value": "page", "property": "object"},
            "page_size": 100,
        }
        total = 0
        while True:
            try:
                data = self._client.search(**kwargs)
            except APIResponseError as e:
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
            kwargs["start_cursor"] = data["next_cursor"]

        logger.info(
            f"[{self.source_name}] Total pages discovered via search: {total}"
        )

    def _selective_pages(self) -> Iterator[IngestionItem]:
        """Yield items for explicitly configured page_ids and database_ids."""
        for page_id in self.page_ids:
            try:
                page = self._client.pages.retrieve(page_id=page_id)
                item = self._page_to_item(page)
                if item:
                    yield item
            except APIResponseError as e:
                logger.error(
                    f"[{self.source_name}] Failed to fetch page {page_id}: {e}"
                )

        for db_id in self.database_ids:
            try:
                db_page_ids = self._query_database(db_id)
                logger.info(
                    f"[{self.source_name}] Database {db_id}: found {len(db_page_ids)} page(s)"
                )
                for pid in db_page_ids:
                    try:
                        page = self._client.pages.retrieve(page_id=pid)
                        item = self._page_to_item(page)
                        if item:
                            yield item
                    except APIResponseError as e:
                        logger.error(
                            f"[{self.source_name}] Failed to fetch page {pid}: {e}"
                        )
            except APIResponseError as e:
                logger.error(
                    f"[{self.source_name}] Failed to query database {db_id}: {e}"
                )

    def _query_database(self, database_id: str) -> List[str]:
        """Return all page IDs from a Notion database using the DataSources API."""
        page_ids = []
        kwargs: Dict[str, Any] = {"page_size": 100}
        while True:
            data = self._client.data_sources.query(database_id, **kwargs)
            for result in data.get("results", []):
                if result.get("object") == "page":
                    page_ids.append(result["id"])
            if not data.get("has_more"):
                break
            kwargs["start_cursor"] = data["next_cursor"]
        return page_ids

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
        created_by = self._resolve_user_name(
            (page.get("created_by") or {}).get("id")
        )
        last_edited_by = self._resolve_user_name(
            (page.get("last_edited_by") or {}).get("id")
        )

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
                "created_by": created_by,
                "last_edited_by": last_edited_by,
            }
        )
        return item

    def _resolve_user_name(self, user_id: Optional[str]) -> Optional[str]:
        """Resolve a Notion user ID to a display name via the Users API.

        Results are cached per connector instance to avoid redundant API calls.
        Returns None if the user_id is missing or the request fails (e.g. 403
        when the integration lacks user information capabilities).
        """
        if not user_id:
            return None
        if user_id in self._user_cache:
            return self._user_cache[user_id]
        try:
            user = self._client.users.retrieve(user_id=user_id)
            name = user.get("name")
        except APIResponseError as e:
            logger.warning(
                f"[{self.source_name}] Could not resolve user {user_id}: {e}"
            )
            name = None
        self._user_cache[user_id] = name
        return name

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
