import io
import logging
import re
import unicodedata
from datetime import UTC, datetime
from typing import Any

from dropbox import Dropbox
from dropbox.exceptions import ApiError, AuthError, HttpError
from dropbox.files import FileMetadata, ListFolderResult
from markitdown import MarkItDown, MarkItDownException

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

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
        paths = cfg.get("paths", [])
        if isinstance(paths, str):
            paths = [p.strip() for p in paths.split(",") if p.strip()]
        self.paths: list[str] = paths or [""]  # "" means Dropbox root

        # Extension filters (mutually exclusive)
        self.include_extensions: set[str] | None = self._parse_str_filter(cfg.get("include_extensions"), ext=True)
        self.exclude_extensions: set[str] | None = self._parse_str_filter(cfg.get("exclude_extensions"), ext=True)
        if self.include_extensions and self.exclude_extensions:
            raise ValueError("Dropbox connector: 'include_extensions' and 'exclude_extensions' are mutually exclusive")

        # Directory filters (mutually exclusive)
        self.include_directories: set[str] | None = self._parse_str_filter(cfg.get("include_directories"))
        self.exclude_directories: set[str] | None = self._parse_str_filter(cfg.get("exclude_directories"))
        if self.include_directories and self.exclude_directories:
            raise ValueError(
                "Dropbox connector: 'include_directories' and 'exclude_directories' are mutually exclusive"
            )

        self.dbx = Dropbox(access_token)
        self.md = MarkItDown()

    @staticmethod
    def _parse_str_filter(value, ext: bool = False) -> set[str] | None:
        """Parse a filter value from config into a set of normalized strings.

        Accepts a comma-separated string or a list/tuple/set. Returns None when
        value is falsy. Raises ValueError for unsupported types.
        """
        if not value:
            return None
        if isinstance(value, str):
            items = [v.strip() for v in value.split(",") if v.strip()]
        elif isinstance(value, list | tuple | set):
            items = [str(v).strip() for v in value if str(v).strip()]
        else:
            raise ValueError(f"Invalid filter config type {type(value).__name__!r}: expected str or list")
        if not items:
            return None
        if ext:
            return {item.lstrip(".").lower() for item in items}
        return {item.lower() for item in items}

    # ------------------------------------------------------------------
    # Filtering helpers
    # ------------------------------------------------------------------

    def _extension_allowed(self, path: str) -> bool:
        """Return True if the file's extension passes the configured filter."""
        ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""
        if self.include_extensions is not None:
            return ext in self.include_extensions
        if self.exclude_extensions is not None:
            return ext not in self.exclude_extensions
        return True

    def _directory_allowed(self, folder_path: str) -> bool:
        """Return True if any ancestor folder name passes the configured directory filter."""
        parts = {p.lower() for p in folder_path.split("/") if p}
        if self.include_directories is not None:
            return bool(parts & self.include_directories)
        if self.exclude_directories is not None:
            return not (parts & self.exclude_directories)
        return True

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
                    parent = entry.path_lower.rsplit("/", 1)[0]
                    if parent and not self._directory_allowed(parent):
                        continue
                    if not self._extension_allowed(entry.path_lower):
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

    def _sanitize_path(self, path: str) -> str:
        """Convert a Dropbox path to a safe, unique item name."""
        path = unicodedata.normalize("NFKD", path)
        path = path.encode("ascii", "ignore").decode("ascii")
        path = re.sub(r"[ \\/]+", "_", path)
        path = re.sub(r"[^a-zA-Z0-9\-_\.]", "", path)
        path = path.strip("_")
        return path[:255] or "dropbox_file"

    def get_item_name(self, item: IngestionItem) -> str:
        return self._sanitize_path(item.source_ref)

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        return {"file_path": item.source_ref}
