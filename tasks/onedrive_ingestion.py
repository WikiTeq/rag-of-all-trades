import logging
import re
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from llama_index.core import Document
from llama_index.readers.microsoft_onedrive import OneDriveReader

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.config import parse_bool

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class OneDriveIngestionJob(IngestionJob):
    """Ingestion connector for Microsoft OneDrive for Business.

    Uses the LlamaIndex OneDrive reader for file discovery and content fetching.
    Requires App authentication (client credentials): client_id, client_secret,
    tenant_id, and userprincipalname.

    Configuration (config.yaml):
        - config.client_id: Azure app registration client ID (required)
        - config.client_secret: Azure app registration client secret (required)
        - config.tenant_id: Azure tenant ID (required)
        - config.userprincipalname: User principal name / email of the OneDrive owner (required)
        - config.folder_id: OneDrive folder ID to ingest (optional)
        - config.folder_path: Relative path of the OneDrive folder to ingest (optional)
        - config.file_ids: Comma-separated OneDrive file IDs to ingest (optional)
        - config.file_paths: Comma-separated OneDrive file paths to ingest (optional)
        - config.mime_types: Comma-separated MIME types to filter (optional, default: all)
        - config.recursive: Traverse subfolders recursively (optional, default: true)
    """

    @property
    def source_type(self) -> str:
        return "onedrive"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        # Required App auth credentials for OneDrive for Business
        self.client_id = (cfg.get("client_id") or "").strip()
        if not self.client_id:
            raise ValueError("client_id is required in OneDrive connector config")

        self.client_secret = (cfg.get("client_secret") or "").strip()
        if not self.client_secret:
            raise ValueError("client_secret is required in OneDrive connector config")

        self.tenant_id = (cfg.get("tenant_id") or "").strip()
        if not self.tenant_id:
            raise ValueError("tenant_id is required in OneDrive connector config")

        # The UPN of the user whose OneDrive will be accessed via app credentials
        self.userprincipalname = (cfg.get("userprincipalname") or "").strip()
        if not self.userprincipalname:
            raise ValueError("userprincipalname is required in OneDrive connector config")

        # Optional content selectors — if none are set, all files from root are loaded
        self.folder_id: str | None = (cfg.get("folder_id") or "").strip() or None
        self.folder_path: str | None = (cfg.get("folder_path") or "").strip() or None
        self.file_ids: list[str] | None = self._parse_config_list(cfg.get("file_ids"))
        self.file_paths: list[str] | None = self._parse_config_list(cfg.get("file_paths"))
        self.mime_types: list[str] | None = self._parse_config_list(cfg.get("mime_types"))

        self.recursive: bool = parse_bool(cfg.get("recursive"), default=True)

        self._reader: OneDriveReader = OneDriveReader(
            client_id=self.client_id,
            client_secret=self.client_secret,
            tenant_id=self.tenant_id,
            userprincipalname=self.userprincipalname,
            folder_id=self.folder_id,
            file_ids=self.file_ids,
            folder_path=self.folder_path,
            file_paths=self.file_paths,
        )

        logger.info(
            f"Initialized OneDrive connector (tenant=***{self.tenant_id[-4:]}, "
            f"user=***{self.userprincipalname[-4:]}, folder_path={self.folder_path!r}, "
            f"folder_id={self.folder_id!r}, recursive={self.recursive})"
        )

    def list_items(self) -> Iterator[IngestionItem]:
        """Load all OneDrive documents and yield one IngestionItem per document."""
        logger.info(f"[{self.source_name}] Loading documents from OneDrive")

        try:
            # load_data authenticates, discovers files, downloads to a temp dir,
            # and returns LlamaIndex Document objects with content + metadata
            docs = self._reader.load_data(
                mime_types=self.mime_types,
                recursive=self.recursive,
            )
        except Exception:
            logger.exception(f"[{self.source_name}] Failed to load documents from OneDrive")
            raise

        # reader may return None instead of raising on certain 404/empty responses
        if not docs:
            logger.info(f"[{self.source_name}] Found 0 document(s)")
            return

        logger.info(f"[{self.source_name}] Found {len(docs)} document(s)")

        for doc in docs:
            file_id = doc.metadata.get("file_id") or doc.doc_id or ""
            last_modified = self._parse_last_modified(doc.metadata.get("last_modified_datetime"))
            if last_modified is None:
                logger.warning(
                    f"[{self.source_name}] Could not parse last_modified_datetime for file_id={file_id!r}, using now"
                )
                last_modified = datetime.now(UTC)
            # Multi-page files (e.g. PDFs) produce one Document per page, all sharing
            # the same file_id. Appending page_label makes the item ID stable and unique
            # per page so the base class dedup logic tracks each page independently.
            page_label = doc.metadata.get("page_label") or ""
            page_suffix = f":{page_label}" if page_label else ""
            yield IngestionItem(
                id=f"onedrive:{file_id}{page_suffix}",
                source_ref=doc,
                last_modified=last_modified,
            )

    def get_raw_content(self, item: IngestionItem) -> str:
        """Return the text content of the OneDrive document."""
        doc: Document = item.source_ref

        # Cache file metadata for use in get_document_metadata()
        item._metadata_cache["file_path"] = doc.metadata.get("file_path") or ""
        item._metadata_cache["file_name"] = doc.metadata.get("file_name") or ""
        item._metadata_cache["file_id"] = doc.metadata.get("file_id") or ""

        return doc.text or ""

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe name for the OneDrive document."""
        doc: Document = item.source_ref
        # page_label is appended for the same reason as in list_items(): multi-page files
        # share a file_path, so the name must include the page to be unique in the DB.
        file_path = doc.metadata.get("file_path") or doc.metadata.get("file_id") or item.id
        page_label = doc.metadata.get("page_label") or ""
        suffix = f":{page_label}" if page_label else ""
        safe = re.sub(r"[^\w-]", "_", f"{file_path}{suffix}")
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
        # Extend base metadata with OneDrive-specific file fields
        metadata.update(
            {
                "file_path": item._metadata_cache.get("file_path", ""),
                "file_name": item._metadata_cache.get("file_name", ""),
                "file_id": item._metadata_cache.get("file_id", ""),
            }
        )
        return metadata

    @staticmethod
    def _parse_last_modified(raw_ts: str | None) -> datetime | None:
        if not raw_ts:
            return None
        try:
            dt = datetime.fromisoformat(raw_ts)
            return dt.astimezone(UTC) if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_config_list(value: str | None) -> list[str] | None:
        """Parse a comma-separated config value into a list, or return None if empty."""
        if not value:
            return None
        items = [v.strip() for v in str(value).split(",") if v.strip()]
        return items or None
