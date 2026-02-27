import logging
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from llama_index.core import SimpleDirectoryReader
from pydantic import BaseModel, field_validator

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class DirectoryConnectorConfig(BaseModel):
    """Pydantic model validating the 'config' block of a directory connector."""

    path: Path
    recursive: bool = True
    exclude_hidden: bool = True
    exclude_empty: bool = False
    num_files_limit: Optional[int] = None
    encoding: str = "utf-8"
    filter: Optional[list[str]] = None

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

    @field_validator("filter", mode="before")
    @classmethod
    def normalize_filter(cls, v):
        """Parse filter into a sorted list of dot-prefixed lowercase extensions.

        Accepts a comma-separated string ("txt,md") or a YAML list (["txt", "md"]).
        """
        if v is None:
            return None

        values = v.split(",") if isinstance(v, str) else v

        exts = []
        for item in values:
            normalized = str(item).strip().lower()
            if not normalized:
                continue
            if not normalized.startswith("."):
                normalized = f".{normalized}"
            exts.append(normalized)

        return sorted(set(exts)) or None


class DirectoryIngestionJob(IngestionJob):
    """Ingest files from a local directory using LlamaIndex SimpleDirectoryReader."""

    @property
    def source_type(self) -> str:
        return "directory"

    def __init__(self, config):
        super().__init__(config)

        self.connector_config = DirectoryConnectorConfig(**(config.get("config", {})))
        self.reader = self._build_directory_reader()

    def _build_directory_reader(self) -> SimpleDirectoryReader:
        cfg = self.connector_config
        return SimpleDirectoryReader(
            input_dir=str(cfg.path),
            recursive=cfg.recursive,
            required_exts=cfg.filter,
            exclude_hidden=cfg.exclude_hidden,
            exclude_empty=cfg.exclude_empty,
            num_files_limit=cfg.num_files_limit,
            encoding=cfg.encoding,
            errors="ignore",
            raise_on_error=True,
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
                path = self.connector_config.path / path
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
            logger.warning(
                "[%s] SimpleDirectoryReader failed: %s", file_path, exc, exc_info=True
            )
            return ""

        merged = "\n\n".join((doc.text or "").strip() for doc in docs if (doc.text or "").strip())
        return merged if merged.strip() else ""

    def get_item_name(self, item: IngestionItem):
        file_path = Path(item.source_ref).resolve()
        try:
            relative_path = file_path.relative_to(self.connector_config.path)
        except ValueError:
            relative_path = file_path.name
        return self.sanitize_path(str(relative_path))
