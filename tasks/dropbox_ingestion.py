import io
import logging
from datetime import UTC, datetime
from typing import Any

from dropbox import Dropbox
from dropbox.exceptions import ApiError, AuthError, HttpError
from dropbox.files import FileMetadata, ListFolderResult
from markitdown import MarkItDown, MarkItDownException

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.filters import path_accepted
from utils.parse import parse_list
from utils.text import sanitize_ascii_key

logger = logging.getLogger(__name__)


class DropboxIngestionJob(IngestionJob):
    """Ingestion job for Dropbox files using the official Dropbox Python SDK."""

    @property
    def source_type(self) -> str:
        return "dropbox"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        access_token = cfg.get("access_token", "")
        if not access_token:
            raise ValueError("Dropbox connector requires 'access_token' in config")

        # Folder paths to traverse; empty list means root (ingest everything)
        paths = parse_list(cfg.get("paths", []))
        self.paths: list[str] = paths or [""]  # "" means Dropbox root

        # Extension filters (mutually exclusive) — stored without leading dot, lowercase
        self.include_extensions: set[str] | None = {
            e.lstrip(".") for e in parse_list(cfg.get("include_extensions"), lower=True)
        } or None
        self.exclude_extensions: set[str] | None = {
            e.lstrip(".") for e in parse_list(cfg.get("exclude_extensions"), lower=True)
        } or None
        if self.include_extensions and self.exclude_extensions:
            raise ValueError("Dropbox connector: 'include_extensions' and 'exclude_extensions' are mutually exclusive")

        # Directory filters (mutually exclusive)
        self.include_directories: set[str] | None = set(parse_list(cfg.get("include_directories"), lower=True)) or None
        self.exclude_directories: set[str] | None = set(parse_list(cfg.get("exclude_directories"), lower=True)) or None
        if self.include_directories and self.exclude_directories:
            raise ValueError(
                "Dropbox connector: 'include_directories' and 'exclude_directories' are mutually exclusive"
            )

        self.dbx = Dropbox(access_token)
        self.md = MarkItDown()

    # ------------------------------------------------------------------
    # Folder traversal
    # ------------------------------------------------------------------

    def _list_folder_recursive(self, folder_path: str):
        """Yield FileMetadata entries recursively under folder_path using cursor pagination."""
        try:
            result: ListFolderResult = self.dbx.files_list_folder(folder_path, recursive=True)
        except AuthError as e:
            logger.error(f"Dropbox auth error while listing '{folder_path}': {e}")
            return
        except ApiError as e:
            logger.error(f"Dropbox API error while listing '{folder_path}': {e}")
            return

        while True:
            for entry in result.entries:
                if isinstance(entry, FileMetadata):
                    if not path_accepted(
                        entry.path_lower,
                        include_extensions={f".{e}" for e in self.include_extensions}
                        if self.include_extensions
                        else None,
                        exclude_extensions={f".{e}" for e in self.exclude_extensions}
                        if self.exclude_extensions
                        else None,
                    ):
                        continue
                    # Directory filter: match any ancestor folder name (not prefix)
                    parent = entry.path_lower.rsplit("/", 1)[0]
                    if parent:
                        parts = {p for p in parent.split("/") if p}
                        if self.include_directories is not None and not (parts & self.include_directories):
                            continue
                        if self.exclude_directories is not None and parts & self.exclude_directories:
                            continue
                    yield entry

            if not result.has_more:
                break

            try:
                result = self.dbx.files_list_folder_continue(result.cursor)
            except ApiError as e:
                logger.error(f"Dropbox pagination error for '{folder_path}': {e}")
                break

    def list_items(self):
        """Yield IngestionItems for all allowed files across all configured paths."""
        seen_ids: set[str] = set()
        for folder_path in self.paths:
            # Normalise: root must be "" for the SDK, sub-paths must start with "/"
            normalised = folder_path.strip()
            if normalised == "/" or normalised == "":
                normalised = ""
            elif not normalised.startswith("/"):
                normalised = "/" + normalised

            for entry in self._list_folder_recursive(normalised):
                if entry.id in seen_ids:
                    continue
                seen_ids.add(entry.id)

                last_modified: datetime | None = None
                if entry.client_modified:
                    lm = entry.client_modified
                    if lm.tzinfo is None:
                        lm = lm.replace(tzinfo=UTC)
                    last_modified = lm

                yield IngestionItem(
                    id=entry.id,
                    source_ref=entry.path_display,
                    last_modified=last_modified,
                )

    # ------------------------------------------------------------------
    # Content extraction
    # ------------------------------------------------------------------

    def get_raw_content(self, item: IngestionItem) -> str:
        path: str = item.source_ref
        try:
            _, response = self.dbx.files_download(path)
            content_bytes: bytes = response.content
        except (ApiError, HttpError) as e:
            logger.error(f"[{path}] Dropbox download failed: {e}")
            return ""

        stream = io.BytesIO(content_bytes)
        try:
            result = self.md.convert_stream(stream)
            text = result.text_content or ""
            if text.strip():
                return text
            logger.debug(f"[{path}] Empty markdown result, falling back to raw text")
            return content_bytes.decode("utf-8", errors="ignore")
        except MarkItDownException as conversion_error:
            logger.warning(f"[{path}] Markdown conversion failed: {conversion_error}. Using raw text.")
            return content_bytes.decode("utf-8", errors="ignore")

    # ------------------------------------------------------------------
    # Naming
    # ------------------------------------------------------------------

    def get_item_name(self, item: IngestionItem) -> str:
        return sanitize_ascii_key(item.source_ref).strip("_") or "dropbox_file"

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        return {"file_path": item.source_ref}
