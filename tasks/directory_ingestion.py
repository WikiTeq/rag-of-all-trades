import logging
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from llama_index.core import SimpleDirectoryReader

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)


class DirectoryIngestionJob(IngestionJob):
    """Ingest files from a local directory using LlamaIndex SimpleDirectoryReader."""

    @property
    def source_type(self) -> str:
        return "directory"

    def __init__(self, config):
        super().__init__(config)

        cfg = config.get("config", {})
        directory = cfg.get("path")
        if not directory:
            raise ValueError("path is required in directory connector config")

        self.directory = Path(directory).expanduser().resolve()
        if not self.directory.is_dir():
            raise ValueError(f"Directory does not exist or is not a directory: {self.directory}")

        self.recursive = self._as_bool(cfg.get("recursive"), default=True)
        self.exclude_hidden = self._as_bool(cfg.get("exclude_hidden"), default=True)
        self.exclude_empty = self._as_bool(cfg.get("exclude_empty"), default=False)
        self.num_files_limit = self._parse_num_files_limit(cfg.get("num_files_limit"))
        self.encoding = cfg.get("encoding", "utf-8")
        self.required_exts = self._parse_required_exts(cfg.get("filter"))
        self.errors = "ignore"
        self.raise_on_error = True
        self.reader = self._build_directory_reader()

    def _as_bool(self, value, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "yes", "on"}:
                return True
            if lowered in {"0", "false", "no", "off"}:
                return False
        return bool(value)

    def _parse_num_files_limit(self, value) -> Optional[int]:
        if value is None or value == "":
            return None
        parsed = int(value)
        if parsed <= 0:
            raise ValueError("num_files_limit must be positive when provided")
        return parsed

    def _parse_required_exts(self, raw_filter) -> Optional[list[str]]:
        """Parse connector `filter` into SimpleDirectoryReader `required_exts` format."""
        if raw_filter is None:
            return None

        if isinstance(raw_filter, str):
            values = raw_filter.split(",")
        elif isinstance(raw_filter, (list, tuple, set)):
            values = raw_filter
        else:
            values = [str(raw_filter)]

        required_exts = []
        for value in values:
            normalized = str(value).strip().lower()
            if not normalized:
                continue
            if not normalized.startswith("."):
                normalized = f".{normalized}"
            required_exts.append(normalized)

        return sorted(set(required_exts)) or None

    def _build_directory_reader(self) -> SimpleDirectoryReader:
        return SimpleDirectoryReader(
            input_dir=str(self.directory),
            recursive=self.recursive,
            required_exts=self.required_exts,
            exclude_hidden=self.exclude_hidden,
            exclude_empty=self.exclude_empty,
            num_files_limit=self.num_files_limit,
            encoding=self.encoding,
            errors=self.errors,
            raise_on_error=self.raise_on_error,
        )

    def sanitize_path(self, path: str) -> str:
        """Normalize a relative path into a filesystem-safe key."""
        path = unicodedata.normalize("NFKD", path)
        path = path.encode("ascii", "ignore").decode("ascii")
        path = re.sub(r"[ \\/]+", "_", path)
        path = re.sub(r"[^a-zA-Z0-9\-_\.]", "", path)
        return path[:255]

    def _get_discovered_paths(self) -> Iterable[Path]:
        for resource in sorted(self.reader.list_resources()):
            path = Path(resource).expanduser()
            if not path.is_absolute():
                path = self.directory / path
            yield path.resolve()

    def list_items(self):
        for file_path in self._get_discovered_paths():
            if not file_path.is_file():
                continue

            try:
                modified_at = datetime.fromtimestamp(file_path.stat().st_mtime)
            except OSError as exc:
                logger.warning(f"Failed to read file metadata for {file_path}: {exc}")
                continue

            yield IngestionItem(
                id=f"file://{file_path}",
                source_ref=file_path,
                last_modified=modified_at,
            )

    def _load_documents_for_path(self, file_path: Path):
        """Load one file using the initialized directory reader context."""
        return self.reader.load_resource(str(file_path))

    def get_raw_content(self, item: IngestionItem):
        file_path = Path(item.source_ref)

        try:
            docs = self._load_documents_for_path(file_path)
        except Exception as exc:
            logger.warning(f"[{file_path}] SimpleDirectoryReader failed: {exc}. Trying raw text fallback.")
            docs = []

        merged = "\n\n".join((doc.text or "").strip() for doc in docs if (doc.text or "").strip())
        if merged.strip():
            return merged

        try:
            return file_path.read_text(encoding=self.encoding, errors=self.errors)
        except Exception as exc:
            logger.error(f"[{file_path}] Failed to read file with fallback: {exc}")
            return ""

    def get_item_name(self, item: IngestionItem):
        file_path = Path(item.source_ref).resolve()
        try:
            relative_path = file_path.relative_to(self.directory)
        except ValueError:
            relative_path = file_path.name
        return self.sanitize_path(str(relative_path))
