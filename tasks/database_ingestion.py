"""Database ingestion connector for MySQL and PostgreSQL.

Uses SQLAlchemy directly to execute a pre-configured SQL SELECT query and
ingest each row as a document into the vector store. This connector does
NOT use LlamaIndex's ``DatabaseReader``: that reader joins every returned
column into a single ``Document`` text blob (e.g. ``"id: 1, title: ..."``),
which doesn't map onto the per-column ``id``/``title``/``updated_at``/
``content`` fields an ``IngestionItem`` needs, so raw SQLAlchemy execution is
used instead.

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
from collections.abc import Iterator
from typing import Any

from sqlalchemy import create_engine, text

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.parse import parse_list, parse_timestamp
from utils.text import slugify

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
            raise ValueError("config.type must be 'postgres' or 'mysql' in database connector config")

        self.connection_string = cfg.get("connection_string", "").strip()
        if not self.connection_string:
            raise ValueError("connection_string is required in database connector config")

        self.query = cfg.get("query", "").strip()
        if not self.query:
            raise ValueError("query is required in database connector config")
        self._validate_select_query(self.query)

        self.metadata_columns: list[str] = parse_list(cfg.get("metadata_columns", ""))

        logger.info(
            f"[{self.source_name}] Initialized database connector "
            f"(type={self.db_type}, query={self.query!r}, "
            f"metadata_columns={self.metadata_columns})"
        )

    def list_items(self) -> Iterator[IngestionItem]:
        """Execute the configured SQL query and yield one IngestionItem per row.

        Rows are streamed row-by-row from the SQLAlchemy result cursor rather
        than materialized with ``fetchall()``, so the whole result set is
        never held in memory as a Python list at once (note: this is
        client-side iteration, not necessarily a server-side streaming
        cursor — that depends on driver-level configuration, which is out of
        scope here). Required-column validation runs against the result's
        column names immediately after ``execute()``, so it still catches a
        mis-aliased query even when the query returns zero rows.

        The inner ``_fetch_rows()`` generator is held in a local variable and
        explicitly closed in ``finally`` so the underlying DB connection and
        engine are disposed promptly whenever *this* generator (``list_items()``
        itself) is closed or garbage-collected — e.g. via an explicit
        ``.close()`` on it, or the interpreter finalizing it once nothing
        references it anymore. Note this is not the same as the caller simply
        `break`-ing out of a ``for row in job.list_items(): ...`` loop: a bare
        `break` does not call ``.close()`` on the iterator per Python's
        iterator protocol, so cleanup in that case still depends on when
        ``list_items()`` itself gets finalized (typically prompt under
        CPython's refcounting, but not a language guarantee).
        """
        logger.info(f"[{self.source_name}] Executing query: {self.query!r}")

        count = 0
        rows = None
        try:
            rows = self._fetch_rows()
            for row in rows:
                yield IngestionItem(
                    id=f"database:{self.source_name}:{row['id']}",
                    source_ref=row,
                    last_modified=parse_timestamp(row.get("updated_at")),
                )
                count += 1
        except Exception:
            logger.exception(f"[{self.source_name}] Failed to execute query")
            raise
        finally:
            if rows is not None:
                rows.close()

        logger.info(f"[{self.source_name}] Found {count} row(s)")

    def get_raw_content(self, item: IngestionItem) -> str:
        """Return the content column value for the row."""
        row = item.source_ref
        return str(row.get("content", "") or "")

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe name derived from the row title."""
        row = item.source_ref
        title = str(row.get("title", "") or item.id)
        return slugify(title)

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        """Return extra metadata fields: title, id, db_type, and any configured extra columns."""
        row = item.source_ref

        extra: dict[str, Any] = {
            "title": str(row.get("title", "") or ""),
            "id": str(row.get("id", "") or ""),
            "db_type": self.db_type,
        }

        for col in self.metadata_columns:
            if col in row:
                extra[col] = row[col]

        return extra

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_rows(self) -> Iterator[dict[str, Any]]:
        """Execute the SQL query and yield rows as dicts, one at a time.

        Validates required columns from the result's column names right
        after ``execute()``, before consuming any rows, so an empty result
        set still catches a mis-aliased query. The engine is disposed in
        ``finally`` so this runs whether the generator is fully consumed,
        errors out, or is closed early — ``list_items()`` explicitly closes
        this generator in its own ``finally``, so disposal happens promptly
        on that path rather than only whenever this generator eventually
        gets garbage-collected.
        """
        engine = create_engine(self.connection_string)
        try:
            with engine.connect() as conn:
                result = conn.execute(text(self.query))
                keys = list(result.keys())

                missing = REQUIRED_COLUMNS - set(keys)
                if missing:
                    raise ValueError(
                        f"Query result is missing required columns: {sorted(missing)}. "
                        f"The query must return: {sorted(REQUIRED_COLUMNS)}. "
                        f"Use SQL AS aliases to map your schema."
                    )

                for row in result:
                    yield dict(zip(keys, row, strict=True))
        finally:
            engine.dispose()

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
