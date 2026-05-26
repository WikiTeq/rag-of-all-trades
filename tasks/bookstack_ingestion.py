import logging
from collections.abc import Iterator
from typing import Any

import requests

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.http import RetrySession
from utils.parse import parse_list, parse_timestamp
from utils.text import html_to_markdown, slugify

logger = logging.getLogger(__name__)


class BookStackClient:
    """HTTP client for the BookStack REST API.

    Handles authentication, request execution with retry logic, and pagination.
    """

    PAGE_SIZE = 100

    def __init__(self, base_url: str, token_id: str, token_secret: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._auth_header = {"Authorization": f"Token {token_id}:{token_secret}"}
        self._session = RetrySession()

    def get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self.base_url}/api/{path.lstrip('/')}"
        response = self._session.get(url, headers=self._auth_header, params=params)
        if response.status_code in (requests.codes.unauthorized, requests.codes.forbidden):
            raise PermissionError(f"BookStack API auth failed ({response.status_code}) for {url}")
        response.raise_for_status()
        return response.json()

    def paginate(self, endpoint: str) -> Iterator[dict]:
        """Yield all items from a paginated BookStack list endpoint."""
        offset = 0
        while True:
            data = self.get(endpoint, params={"count": self.PAGE_SIZE, "offset": offset})
            items = data.get("data", [])
            yield from items
            if len(items) < self.PAGE_SIZE:
                break
            offset += len(items)


class BookStackIngestionJob(IngestionJob):
    """Ingestion connector for BookStack instances.

    Fetches shelves, books, chapters, and pages via the BookStack REST API
    and stores them in the vector store.

    Configuration (config.yaml):
        - config.base_url: BookStack instance base URL (required)
        - config.token_id: API token ID (required)
        - config.token_secret: API token secret (required)
        - config.item_types: item types to ingest, comma-separated or list
          (optional, default: shelves,books,chapters,pages)
    """

    VALID_ITEM_TYPES = {"shelves", "books", "chapters", "pages"}

    @property
    def source_type(self) -> str:
        return "bookstack"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        base_url = cfg.get("base_url", "").rstrip("/")
        if not base_url:
            raise ValueError("base_url is required in BookStack connector config")

        token_id = cfg.get("token_id", "").strip()
        if not token_id:
            raise ValueError("token_id is required in BookStack connector config")

        token_secret = cfg.get("token_secret", "").strip()
        if not token_secret:
            raise ValueError("token_secret is required in BookStack connector config")

        raw_types = cfg.get("item_types", list(self.VALID_ITEM_TYPES))
        self.item_types = parse_list(raw_types, lower=True)
        invalid = set(self.item_types) - self.VALID_ITEM_TYPES
        if invalid:
            raise ValueError(f"Invalid item_types: {invalid}. Must be subset of {self.VALID_ITEM_TYPES}")
        if not self.item_types:
            raise ValueError("item_types must not be empty")

        self._client = BookStackClient(base_url, token_id, token_secret)

    def list_items(self) -> Iterator[IngestionItem]:
        for item_type in self.item_types:
            logger.info(f"[{self.source_name}] Listing {item_type}")
            count = 0
            for item in self._client.paginate(item_type):
                updated_at = parse_timestamp(item.get("updated_at"))
                yield IngestionItem(
                    id=f"bookstack:{item_type}:{item['id']}",
                    source_ref={"type": item_type, "data": item},
                    last_modified=updated_at,
                )
                count += 1
            logger.info(f"[{self.source_name}] Found {count} {item_type}")

    def get_raw_content(self, item: IngestionItem) -> str:
        item_type: str = item.source_ref["type"]
        data: dict = item.source_ref["data"]

        name = data.get("name", "") or ""
        description = data.get("description", "") or data.get("description_html", "") or ""
        url = (
            f"{self._client.base_url}/link/{data['id']}"
            if item_type == "pages"
            else f"{self._client.base_url}/{item_type}/{data['slug']}"
        )
        item._metadata_cache["url"] = url
        item._metadata_cache["title"] = name

        if item_type == "pages":
            detail = self._client.get(f"pages/{data['id']}")
            item._metadata_cache["detail"] = detail
            markdown = detail.get("markdown", "") or ""
            if markdown.strip():
                content = markdown
            else:
                raw_html = detail.get("raw_html", "") or ""
                content = html_to_markdown(raw_html) if raw_html.strip() else ""
        else:
            content = html_to_markdown(description) if description.strip() else ""

        parts = [f"# {name}"] if name else []
        if content:
            parts.append(content)

        return "\n\n".join(parts)

    def get_item_checksum(self, item: IngestionItem) -> str | None:
        data: dict = item.source_ref["data"]
        updated_at = data.get("updated_at", "")
        if updated_at:
            return f"{data['id']}:{updated_at}"
        return None

    def get_item_name(self, item: IngestionItem) -> str:
        item_type: str = item.source_ref["type"]
        data: dict = item.source_ref["data"]
        name = data.get("name", "") or ""
        return slugify(f"bookstack-{item_type}-{data['id']}-{name}", max_len=255)

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        item_type: str = item.source_ref["type"]
        data: dict = item.source_ref["data"]
        detail: dict = item._metadata_cache.get("detail", {})

        extra: dict[str, Any] = {
            "item_type": item_type,
            "title": item._metadata_cache.get("title", ""),
            "url": item._metadata_cache.get("url", ""),
            "updated_at": str(data.get("updated_at", "") or ""),
            "book_id": str(data.get("book_id", "") or ""),
            "chapter_id": str(data.get("chapter_id", "") or ""),
            "shelf_id": str(data.get("shelf_id", "") or ""),
        }

        if item_type == "pages":
            owner = detail.get("owned_by") or data.get("owned_by") or {}
            editor = detail.get("updated_by") or {}
            extra["owner"] = owner.get("name", "") if isinstance(owner, dict) else str(owner)
            extra["editor"] = editor.get("name", "") if isinstance(editor, dict) else str(editor)
            draft = detail.get("draft", data.get("draft", ""))
            extra["draft"] = str(draft) if draft != "" else ""
            tags = detail.get("tags") or data.get("tags") or []
            extra["tags"] = ",".join(t.get("name", "") for t in tags if isinstance(t, dict))

        if item_type == "shelves":
            tags = data.get("tags") or []
            extra["tags"] = ",".join(t.get("name", "") for t in tags if isinstance(t, dict))

        return extra
