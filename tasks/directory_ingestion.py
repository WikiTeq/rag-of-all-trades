from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

from llama_index.core import SimpleDirectoryReader
from pydantic import BaseModel, field_validator

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.logger import logging
from utils.parse import parse_list
from utils.text import sanitize_ascii_key

# Configure logging
logger = logging.getLogger(__name__)


class DirectoryConnectorConfig(BaseModel):
    """Pydantic model validating the 'config' block of a directory connector."""

    path: Path
    recursive: bool = True
    exclude_hidden: bool = True
    exclude_empty: bool = False
    num_files_limit: int | None = None
    encoding: str = "utf-8"
    required_exts: list[str] | None = None

    model_config = {"extra": "ignore"}

    # --- validators ---

    @field_validator("path", mode="before")
    @classmethod
    def resolve_path(cls, v):
        p = Path(v).expanduser().resolve()
        if not p.is_dir():
            raise ValueError(f"Directory does not exist or is not a directory: {p}")
        return p

    @field_validator("num_files_limit", mode="before")
    @classmethod
    def validate_num_files_limit(cls, v):
        if v is None or v == "":
            return None
        parsed = int(v)
        if parsed <= 0:
            raise ValueError("num_files_limit must be positive when provided")
        return parsed

    @field_validator("required_exts", mode="before")
    @classmethod
    def normalize_required_exts(cls, v):
        """Parse required_exts into sorted dot-prefixed extensions.

        Accepts a comma-separated string ("txt,md") or a YAML list (["txt", "md"]).
        """
        values = parse_list(v, lower=True)
        if not values:
            return None

        exts = {v if v.startswith(".") else f".{v}" for v in values}
        return sorted(exts) or None


class DirectoryIngestionJob(IngestionJob):
    """Ingest files from a local directory using LlamaIndex SimpleDirectoryReader."""

    @property
    def source_type(self) -> str:
        return "directory"

    def __init__(self, config: dict):
        super().__init__(config)

        self.connector_config = DirectoryConnectorConfig(**(config.get("config", {})))
        self.reader = self._build_directory_reader()

    def _build_directory_reader(self) -> SimpleDirectoryReader:
        cfg = self.connector_config
        # errors="ignore" drops invalid bytes during text decode; raise_on_error=True
        # raises on reader-level failures (e.g. missing file). Binary parsers (PDF, images)
        # do not use encoding/errors, so this combination is intentional.
        return SimpleDirectoryReader(
            input_dir=str(cfg.path),
            recursive=cfg.recursive,
            required_exts=cfg.required_exts,
            exclude_hidden=cfg.exclude_hidden,
            exclude_empty=cfg.exclude_empty,
            num_files_limit=cfg.num_files_limit,
            encoding=cfg.encoding,
            errors="ignore",
            raise_on_error=True,
        )

    def _sanitize_path(self, path: str) -> str:
        """Normalize a relative path into a filesystem-safe key."""
        return sanitize_ascii_key(path, max_len=255)

    def _get_discovered_paths(self) -> Iterable[Path]:
        """Yield resolved file paths under the configured directory.

        Paths that resolve outside the configured base (e.g. symlinks to external
        files) are skipped with a warning to avoid ingesting unintended content.
        """
        base = self.connector_config.path.resolve()
        for resource in sorted(self.reader.list_resources()):
            path = Path(resource).expanduser()
            if not path.is_absolute():
                path = base / path
            path = path.resolve()
            try:
                path.relative_to(base)
            except ValueError:
                logger.warning("Skipping path outside configured directory: %s", path)
                continue
            yield path

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
            logger.warning("[%s] SimpleDirectoryReader failed: %s", file_path, exc, exc_info=True)
            return ""

        merged = "\n\n".join((doc.text or "").strip() for doc in docs if (doc.text or "").strip())
        return merged if merged.strip() else ""

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe name for the item.

        Uses the path relative to the configured directory. If the path is outside
        the base (e.g. symlink escape), falls back to the bare filename; callers
        should be aware this can collide if multiple such files share the same name.
        """
        file_path = Path(item.source_ref).resolve()
        try:
            relative_path = file_path.relative_to(self.connector_config.path)
        except ValueError:
            logger.warning("Path %s is outside configured directory, falling back to filename", file_path)
            relative_path = file_path.name
        return self._sanitize_path(str(relative_path))
