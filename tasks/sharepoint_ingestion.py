import logging
import re
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from llama_index.readers.microsoft_sharepoint import SharePointReader, SharePointType

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)


class SharePointIngestionJob(IngestionJob):
    """Ingestion connector for Microsoft SharePoint.

    Fetches files from SharePoint document libraries or site pages using the
    LlamaIndex SharePoint reader and stores them in the vector store.

    Supports two modes (configured via ``sharepoint_type``):
      - ``file`` (default): load files from a SharePoint drive / folder
      - ``page``: load SharePoint site pages

    Configuration (config.yaml):
        - config.client_id: Azure app client ID (required)
        - config.client_secret: Azure app client secret (required)
        - config.tenant_id: Azure tenant ID (required)
        - config.sharepoint_site_name: SharePoint site name (optional)
        - config.sharepoint_site_id: SharePoint site ID (optional; alternative to site_name)
        - config.sharepoint_host_name: SharePoint host, e.g. contoso.sharepoint.com (optional)
        - config.sharepoint_relative_url: Relative URL of the site, e.g. /sites/MySite (optional)
        - config.sharepoint_folder_path: Folder path within the drive (optional)
        - config.sharepoint_folder_id: Folder ID within the drive (optional)
        - config.drive_name: Name of the document library / drive (optional)
        - config.sharepoint_type: "file" or "page" (optional, default "file")
        - config.recursive: traverse subfolders recursively (optional, default true)
    """

    @property
    def source_type(self) -> str:
        return "sharepoint"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        self.client_id = cfg.get("client_id", "").strip()
        if not self.client_id:
            raise ValueError("client_id is required in SharePoint connector config")

        self.client_secret = cfg.get("client_secret", "").strip()
        if not self.client_secret:
            raise ValueError("client_secret is required in SharePoint connector config")

        self.tenant_id = cfg.get("tenant_id", "").strip()
        if not self.tenant_id:
            raise ValueError("tenant_id is required in SharePoint connector config")

        self.sharepoint_site_name: str | None = cfg.get("sharepoint_site_name", "").strip() or None
        self.sharepoint_site_id: str | None = cfg.get("sharepoint_site_id", "").strip() or None
        self.sharepoint_host_name: str | None = cfg.get("sharepoint_host_name", "").strip() or None
        self.sharepoint_relative_url: str | None = cfg.get("sharepoint_relative_url", "").strip() or None
        self.sharepoint_folder_path: str | None = cfg.get("sharepoint_folder_path", "").strip() or None
        self.sharepoint_folder_id: str | None = cfg.get("sharepoint_folder_id", "").strip() or None
        self.drive_name: str | None = cfg.get("drive_name", "").strip() or None

        _type_map = {"file": SharePointType.DRIVE, "page": SharePointType.PAGE}
        sharepoint_type_raw = cfg.get("sharepoint_type", "file").strip().lower()
        if sharepoint_type_raw not in _type_map:
            raise ValueError(f"Invalid sharepoint_type {sharepoint_type_raw!r}; expected one of {list(_type_map)}")
        self.sharepoint_type = _type_map[sharepoint_type_raw]

        recursive_raw = cfg.get("recursive", True)
        if isinstance(recursive_raw, str):
            self.recursive = recursive_raw.strip().lower() not in ("false", "0", "no")
        else:
            self.recursive = bool(recursive_raw)

        self._reader = SharePointReader(
            client_id=self.client_id,
            client_secret=self.client_secret,
            tenant_id=self.tenant_id,
            sharepoint_site_name=self.sharepoint_site_name,
            sharepoint_site_id=self.sharepoint_site_id,
            sharepoint_host_name=self.sharepoint_host_name,
            sharepoint_relative_url=self.sharepoint_relative_url,
            sharepoint_type=self.sharepoint_type,
        )

        logger.info(
            f"Initialized SharePoint connector: site={self.sharepoint_site_name!r}, "
            f"type={sharepoint_type_raw!r}, recursive={self.recursive}"
        )

    def list_items(self) -> Iterator[IngestionItem]:
        """Load all SharePoint documents/pages and yield one IngestionItem per document."""
        logger.info(f"[{self.source_name}] Loading SharePoint content")

        load_kwargs: dict[str, Any] = {
            k: v
            for k, v in {
                "sharepoint_site_name": self.sharepoint_site_name,
                "sharepoint_folder_path": self.sharepoint_folder_path,
                "sharepoint_folder_id": self.sharepoint_folder_id,
                "drive_name": self.drive_name,
            }.items()
            if v is not None
        }
        if self.sharepoint_type == SharePointType.DRIVE:
            load_kwargs["recursive"] = self.recursive

        docs = self._reader.load_data(**load_kwargs)
        logger.info(f"[{self.source_name}] Loaded {len(docs)} document(s) from SharePoint")

        for doc in docs:
            stable_id = (
                doc.metadata.get("file_id") or doc.id_ or doc.metadata.get("file_path") or doc.metadata.get("file_name")
            )
            item_id = f"sharepoint:{self.source_name}:{stable_id}"
            raw_ts = (
                doc.metadata.get("lastModifiedDateTime")
                or doc.metadata.get("last_modified_datetime")
                or doc.metadata.get("last_modified")
            )
            if isinstance(raw_ts, datetime):
                last_modified = raw_ts.astimezone(UTC)
            elif isinstance(raw_ts, str):
                try:
                    last_modified = datetime.fromisoformat(raw_ts).astimezone(UTC)
                except ValueError:
                    logger.warning(
                        "[%s] Could not parse last_modified %r for %s; falling back to now()",
                        self.source_name,
                        raw_ts,
                        stable_id,
                    )
                    last_modified = datetime.now(UTC)
            else:
                logger.warning(
                    "[%s] No last_modified metadata for %s; falling back to now()",
                    self.source_name,
                    stable_id,
                )
                last_modified = datetime.now(UTC)
            yield IngestionItem(
                id=item_id,
                source_ref=doc,
                last_modified=last_modified,
            )

    def get_raw_content(self, item: IngestionItem) -> str:
        """Return the document text already fetched by the reader."""
        return item.source_ref.text or ""

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe unique name for this item."""
        doc = item.source_ref
        file_path = doc.metadata.get("file_path") or doc.metadata.get("file_name") or doc.id_
        safe = re.sub(r"[^\w\-]", "_", f"{self.source_name}_{file_path}")
        return safe[:255]

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        """Return SharePoint-specific metadata fields."""
        doc = item.source_ref
        return {
            "file_path": doc.metadata.get("file_path", ""),
            "file_name": doc.metadata.get("file_name", ""),
            "url": doc.metadata.get("url", ""),
            "title": doc.metadata.get("title", "") or doc.metadata.get("file_name", ""),
        }
