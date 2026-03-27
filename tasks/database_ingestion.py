"""Database ingestion connector for MySQL and PostgreSQL.

Uses SQLAlchemy to execute a pre-configured SQL SELECT query and ingest each
row as a document into the vector store.

The query MUST return the following columns (use SQL AS aliases if needed):
    - id:         Unique row identifier
    - title:      Human-readable name of the item
    - updated_at: Last modification timestamp (ISO-8601 string or datetime)
    - content:    Main text body to embed

Any additional columns listed in ``metadata_columns`` are stored as metadata.

Example:
    SELECT employee_id AS id, full_name AS title, updated_at, bio AS content,
           department AS department
    FROM employees
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

REQUIRED_COLUMNS = {"id", "title", "updated_at", "content"}


class DatabaseIngestionJob(IngestionJob):
    """Ingestion connector for MySQL and PostgreSQL databases.

    Executes a pre-configured SQL SELECT query and ingests each row as a
    document. The query must return id, title, updated_at, and content columns
    (use SQL AS aliases to map your schema). Any extra columns listed in
    ``metadata_columns`` are stored in document metadata.

    Configuration (config.yaml):
        - config.type:              Database type: "postgres" or "mysql" (required)
        - config.connection_string: SQLAlchemy connection string (required);
                                    use a read-only DB account
        - config.query:             SQL SELECT statement (required); must return
                                    id, title, updated_at, content columns;
                                    non-SELECT statements are rejected at startup
        - config.metadata_columns:  Comma-separated extra columns to store as
                                    metadata (optional)
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
        self._validate_select_query(self.query)

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
        """Execute the configured SQL query and yield one IngestionItem per row."""
        logger.info(f"[{self.source_name}] Executing query: {self.query!r}")

        try:
            rows = self._fetch_rows()
        except Exception as e:
            logger.exception(f"[{self.source_name}] Failed to execute query: {e}")
            raise

        if rows:
            missing = REQUIRED_COLUMNS - set(rows[0].keys())
            if missing:
                raise ValueError(
                    f"Query result is missing required columns: {sorted(missing)}. "
                    f"The query must return: {sorted(REQUIRED_COLUMNS)}. "
                    f"Use SQL AS aliases to map your schema."
                )

        count = 0
        for row in rows:
            yield IngestionItem(
                id=f"database:{self.source_name}:{row['id']}",
                source_ref=row,
                last_modified=self._parse_timestamp(row.get("updated_at")),
            )
            count += 1

        logger.info(f"[{self.source_name}] Found {count} row(s)")

    def get_raw_content(self, item: IngestionItem) -> str:
        """Return the content column value for the row."""
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
    def _validate_select_query(query: str) -> None:
        """Reject any query that is not a SELECT statement."""
        stripped = re.sub(r"/\*.*?\*/", " ", query, flags=re.DOTALL)
        stripped = re.sub(r"--[^\n]*", " ", stripped)
        first_token = stripped.split()[0].upper() if stripped.split() else ""
        if first_token != "SELECT":
            raise ValueError(
                "config.query must be a SELECT statement. "
                "Use read-only database credentials to enforce this at the DB level."
            )

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
