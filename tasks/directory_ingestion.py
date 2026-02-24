import logging
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Set

from markitdown import MarkItDown

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)


class DirectoryIngestionJob(IngestionJob):
    """Ingest files from a local directory and convert them to markdown text."""

    @property
    def source_type(self) -> str:
        return "directory"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})
        directory = cfg.get("path")
        if not directory:
            raise ValueError("path is required in directory connector config")

        self.directory = Path(directory).expanduser().resolve()
        if not self.directory.is_dir():
            raise ValueError(f"Directory does not exist or is not a directory: {self.directory}")

        raw_recursive = cfg.get("recursive", True)
        if isinstance(raw_recursive, str):
            self.recursive = raw_recursive.strip().lower() not in (
                "false",
                "0",
                "no",
            )
        else:
            self.recursive = bool(raw_recursive)
        self.extension_filter = self._parse_extension_filter(cfg.get("filter"))
        self.md = MarkItDown()

    def _parse_extension_filter(self, raw_filter) -> Optional[Set[str]]:
        """Parse comma-separated extension filter into normalized lowercase set."""
        if raw_filter is None:
            return None

        if isinstance(raw_filter, str):
            values = raw_filter.split(",")
        elif isinstance(raw_filter, (list, tuple, set)):
            values = raw_filter
        else:
            values = [str(raw_filter)]

        extensions = {
            str(value).strip().lower().lstrip(".")
            for value in values
            if str(value).strip()
        }
        return extensions or None

    def _matches_filter(self, file_path: Path) -> bool:
        """Return True if file matches the extension filter.
        When a filter is set, files with no extension are excluded.
        """
        if self.extension_filter is None:
            return True
        extension = file_path.suffix.lower().lstrip(".")
        return extension in self.extension_filter

    def _sanitize_path(self, path: str) -> str:
        """Normalize a relative path into a unique, filesystem-safe key.
        Uses double-underscore for path separators to avoid collisions
        (e.g. a/b.txt vs a_b.txt).
        """
        path = unicodedata.normalize("NFKD", path)
        path = path.encode("ascii", "ignore").decode("ascii")
        path = re.sub(r"[/\\]+", "__", path)
        path = re.sub(r" +", "_", path)
        path = re.sub(r"[^a-zA-Z0-9\-_\.]", "", path)
        return path[:255]

    def list_items(self):
        pattern = "**/*" if self.recursive else "*"
        for file_path in sorted(self.directory.glob(pattern)):
            if not file_path.is_file():
                continue
            if not self._matches_filter(file_path):
                continue

            try:
                modified_at = datetime.fromtimestamp(
                    file_path.stat().st_mtime, tz=timezone.utc
                )
            except OSError as exc:
                logger.warning(f"Failed to read file metadata for {file_path}: {exc}")
                continue

            yield IngestionItem(
                id=f"file://{file_path}",
                source_ref=file_path,
                last_modified=modified_at,
            )

    def get_raw_content(self, item: IngestionItem) -> str:
        file_path = Path(item.source_ref)
        try:
            with file_path.open("rb") as f:
                try:
                    result = self.md.convert_stream(f)
                    text = result.text_content or ""
                    if text.strip():
                        return text
                    f.seek(0)
                    return f.read().decode("utf-8", errors="ignore")
                except Exception as exc:
                    logger.warning(
                        f"[{file_path}] Markdown conversion failed: {exc}. Using raw text."
                    )
                    f.seek(0)
                    return f.read().decode("utf-8", errors="ignore")
        except (OSError, PermissionError) as exc:
            logger.error(f"[{file_path}] Failed to read file: {exc}")
            return ""

    def get_item_name(self, item: IngestionItem) -> str:
        file_path = Path(item.source_ref).resolve()
        try:
            relative_path = file_path.relative_to(self.directory)
        except ValueError:
            relative_path = Path(file_path.name)
        return self._sanitize_path(str(relative_path))
