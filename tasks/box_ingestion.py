import logging
import re
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from box_sdk_gen import BoxCCGAuth, BoxClient, CCGConfig
from llama_index.core import Document
from llama_index.readers.box import BoxReader

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class BoxIngestionJob(IngestionJob):
    """Ingestion connector for Box cloud storage.

    Uses the LlamaIndex Box reader for file discovery and content extraction.
    This connector implements CCG (Client Credential Grant) authentication.
    The underlying box-sdk-gen SDK also supports JWT and OAuth 2.0, but those
    auth flows are not yet wired up here.

    Configuration (config.yaml):
        - config.box_client_id: Box app client ID (required)
        - config.box_client_secret: Box app client secret (required)
        - config.box_enterprise_id: Box enterprise ID for CCG auth (required)
        - config.box_user_id: Box user ID for user-level CCG access (optional)
        - config.folder_id: Box folder ID to ingest (optional)
        - config.file_ids: Comma-separated Box file IDs to ingest (optional)
        - config.is_recursive: Traverse subfolders recursively (optional, default false)
    """

    @property
    def source_type(self) -> str:
        return "box"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        self.box_client_id = (cfg.get("box_client_id") or "").strip()
        if not self.box_client_id:
            raise ValueError("box_client_id is required in Box connector config")

        self.box_client_secret = (cfg.get("box_client_secret") or "").strip()
        if not self.box_client_secret:
            raise ValueError("box_client_secret is required in Box connector config")

        self.box_enterprise_id = (cfg.get("box_enterprise_id") or "").strip()
        if not self.box_enterprise_id:
            raise ValueError("box_enterprise_id is required in Box connector config")

        self.box_user_id: str | None = (cfg.get("box_user_id") or "").strip() or None
        self.folder_id: str | None = (cfg.get("folder_id") or "").strip() or None
        self.file_ids: list[str] | None = self._parse_config_list(cfg.get("file_ids"))
        self.is_recursive: bool = BoxIngestionJob._parse_bool(cfg.get("is_recursive"), default=False)

        if self.folder_id is None and self.file_ids is None:
            raise ValueError("Box connector requires either folder_id or file_ids in config")

        ccg_config = CCGConfig(
            client_id=self.box_client_id,
            client_secret=self.box_client_secret,
            enterprise_id=self.box_enterprise_id,
            user_id=self.box_user_id,
        )
        auth = BoxCCGAuth(config=ccg_config)
        self.box_client = BoxClient(auth=auth)
        self.reader = BoxReader(box_client=self.box_client)

        logger.info(
            f"Initialized Box connector (enterprise=***{self.box_enterprise_id[-4:]}, "
            f"folder_id={self.folder_id!r}, is_recursive={self.is_recursive})"
        )

    def list_items(self) -> Iterator[IngestionItem]:
        """Load all Box documents and yield one IngestionItem per document."""
        logger.info(f"[{self.source_name}] Loading documents from Box")

        try:
            docs = self.reader.load_data(
                folder_id=self.folder_id,
                file_ids=self.file_ids,
                is_recursive=self.is_recursive,
            )
        except Exception:
            logger.exception(f"[{self.source_name}] Failed to load documents from Box")
            raise

        if not docs:
            logger.info(f"[{self.source_name}] Found 0 document(s)")
            return

        logger.info(f"[{self.source_name}] Found {len(docs)} document(s)")

        for doc in docs:
            file_id = doc.metadata.get("box_file_id") or doc.doc_id or ""
            last_modified = self._parse_last_modified(
                doc.metadata.get("modified_at") or doc.metadata.get("content_modified_at")
            )
            if last_modified is None:
                logger.warning(f"[{self.source_name}] Could not parse modified_at for file_id={file_id!r}, using now")
                last_modified = datetime.now(UTC)

            page_label = doc.metadata.get("page_label") or ""
            page_suffix = f":{page_label}" if page_label else ""
            yield IngestionItem(
                id=f"box:{file_id}{page_suffix}",
                source_ref=doc,
                last_modified=last_modified,
            )

    def get_raw_content(self, item: IngestionItem) -> str:
        """Return the text content of the Box document."""
        doc: Document = item.source_ref

        item._metadata_cache["box_file_id"] = doc.metadata.get("box_file_id") or ""
        item._metadata_cache["box_file_name"] = doc.metadata.get("name") or ""
        item._metadata_cache["path_collection"] = doc.metadata.get("path_collection") or ""
        item._metadata_cache["page_label"] = doc.metadata.get("page_label") or ""

        return doc.text or ""

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe name for the Box document."""
        doc: Document = item.source_ref
        file_id = doc.metadata.get("box_file_id") or item.id
        file_name = doc.metadata.get("name") or ""
        page_label = doc.metadata.get("page_label") or ""
        safe_page = re.sub(r"[^\w-]", "_", page_label)
        safe_suffix = f":{safe_page}" if page_label else ""
        safe_name = re.sub(r"[^\w-]", "_", f"box_{file_id}_{file_name}")
        max_len = 255 - len(safe_suffix)
        return safe_name[:max_len] + safe_suffix

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        """Return Box-specific metadata fields."""
        return {
            "box_file_id": item._metadata_cache.get("box_file_id", ""),
            "box_file_name": item._metadata_cache.get("box_file_name", ""),
            "path_collection": item._metadata_cache.get("path_collection", ""),
            "page_label": item._metadata_cache.get("page_label", ""),
        }

    def _parse_last_modified(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt
        except ValueError:
            return None

    @staticmethod
    def _parse_bool(value: object, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in ("1", "true", "yes", "on")

    @staticmethod
    def _parse_config_list(value: str | list | None) -> list[str] | None:
        if value is None:
            return None
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        parts = [v.strip() for v in str(value).split(",") if v.strip()]
        return parts if parts else None
