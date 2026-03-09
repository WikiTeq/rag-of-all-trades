"""Database ingestion connector for MySQL and PostgreSQL.

Uses LlamaIndex DatabaseReader (SQLAlchemy-based) to execute a pre-configured
SQL SELECT query and ingest rows as documents into the vector store.

By convention the query should return the following columns:
    - id:         Unique identifier for the row (used as document ID)
    - title:      Human-readable title / name of the item
    - updated_at: Last modification timestamp (ISO-8601 string or datetime)
    - content:    Main text content to embed

These column names are the default convention and can be overridden via
``required_columns`` in config if the source schema uses different names.
Any additional columns listed in ``metadata_columns`` are stored as metadata.
"""

import logging
import re
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

from llama_index.readers.database import DatabaseReader
from sqlalchemy import create_engine, text

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)


class DatabaseIngestionJob(IngestionJob):
    """Ingestion connector for MySQL and PostgreSQL databases.

    Executes a pre-configured SQL SELECT query and ingests each row as a
    document. By convention the query should return: id, title, updated_at,
    content. Any extra columns listed in ``metadata_columns`` are stored in
    the document metadata alongside the base fields.

    Configuration (config.yaml):
        - config.type:              Database type: "postgres" or "mysql" (required)
        - config.connection_string: SQLAlchemy connection string (required)
        - config.query:             SQL SELECT statement (required); should return
                                    the columns listed in required_columns
        - config.required_columns:  Comma-separated column names the query must
                                    return (optional; if omitted, no validation is performed)
        - config.metadata_columns:  Comma-separated list of extra columns to
                                    include in metadata (optional)
        - config.schedules:         Celery schedule in seconds (optional)
    """

    @property
    def source_type(self) -> str:
        return "database"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        self.db_type = cfg.get("type", "").lower().strip()
        if self.db_type not in ("postgres", "mysql"):
            raise ValueError(
                "config.type must be 'postgres' or 'mysql' in database connector config"
            )

        self.connection_string = cfg.get("connection_string", "").strip()
        if not self.connection_string:
            raise ValueError(
                "connection_string is required in database connector config"
            )

        self.query = cfg.get("query", "").strip()
        if not self.query:
            raise ValueError("query is required in database connector config")

        required_cfg = self._parse_list(cfg.get("required_columns", ""))
        self.required_columns: set[str] = set(required_cfg)

        self.metadata_columns: List[str] = self._parse_list(
            cfg.get("metadata_columns", "")
        )

        self._reader = DatabaseReader(uri=self.connection_string)

        logger.info(
            f"[{self.source_name}] Initialized database connector "
            f"(type={self.db_type}, query={self.query!r}, "
            f"metadata_columns={self.metadata_columns})"
        )

    def list_items(self) -> Iterator[IngestionItem]:
        """Execute the configured SQL query and yield one IngestionItem per row.

        Each row dict is stored as ``source_ref`` so that ``get_raw_content``
        and ``get_document_metadata`` can access all columns without re-querying.
        """
        logger.info(f"[{self.source_name}] Executing query: {self.query!r}")

        try:
            rows = self._fetch_rows()
        except Exception as e:
            logger.error(f"[{self.source_name}] Failed to execute query: {e}")
            return

        if self.required_columns and rows:
            missing = self.required_columns - set(rows[0].keys())
            if missing:
                raise ValueError(
                    f"Query result is missing required columns: {sorted(missing)}. "
                    f"Required: {sorted(self.required_columns)}"
                )

        count = 0
        for row in rows:
            row_id = str(row.get("id", count))
            updated_at = self._parse_timestamp(row.get("updated_at"))
            yield IngestionItem(
                id=f"database:{self.source_name}:{row_id}",
                source_ref=row,
                last_modified=updated_at,
            )
            count += 1

        logger.info(f"[{self.source_name}] Found {count} row(s)")

    def get_raw_content(self, item: IngestionItem) -> str:
        """Return the ``content`` column value for the row."""
        row = item.source_ref
        return str(row.get("content", "") or "")

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe name derived from the row title."""
        row = item.source_ref
        title = str(row.get("title", "") or item.id)
        safe = re.sub(r"[^\w\-]", "_", title)
        return safe[:255]

    def get_document_metadata(
        self,
        item: IngestionItem,
        item_name: str,
        checksum: str,
        version: int,
        last_modified: Any,
    ) -> Dict[str, Any]:
        """Build metadata dict with base fields plus any configured extra columns."""
        row = item.source_ref

        metadata = super().get_document_metadata(
            item, item_name, checksum, version, last_modified
        )

        metadata.update(
            {
                "title": str(row.get("title", "") or ""),
                "id": str(row.get("id", "") or ""),
                "db_type": self.db_type,
            }
        )

        for col in self.metadata_columns:
            if col in row:
                metadata[col] = row[col]

        return metadata

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_rows(self) -> List[Dict[str, Any]]:
        """Execute the SQL query and return rows as a list of dicts."""
        engine = create_engine(self.connection_string)
        with engine.connect() as conn:
            result = conn.execute(text(self.query))
            keys = list(result.keys())
            return [dict(zip(keys, row)) for row in result.fetchall()]

    @staticmethod
    def _parse_timestamp(value: Any) -> Optional[datetime]:
        """Convert a string or datetime value to a datetime object."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_list(value: Any) -> List[str]:
        """Parse a comma-separated string or list into a list of stripped strings."""
        if not value:
            return []
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        return [v.strip() for v in str(value).split(",") if v.strip()]
