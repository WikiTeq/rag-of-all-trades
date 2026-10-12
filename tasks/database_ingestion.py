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

import hashlib
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
                                    non-SELECT/WITH statements are rejected at
                                    startup as a best-effort check, not a full
                                    SQL-injection guard — see
                                    _validate_select_query's docstring
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

    def get_item_checksum(self, item: IngestionItem) -> str | None:
        """Return ``"{id}:{updated_at}"`` as a pre-computed checksum.

        Note on what this actually saves for this connector: unlike an
        API-based connector where ``get_raw_content()`` makes a network call,
        this connector's ``list_items()`` already selects ``content`` as part
        of the row and stores it on ``item.source_ref`` — so
        ``get_raw_content()`` here is a free dict lookup, not an extra fetch.
        What this checksum actually avoids is computing an MD5 hash over
        ``content`` locally on every run for every row (real, if modest, CPU
        cost at scale); it does not avoid a DB round-trip.

        Returns ``None`` (falling back to the base class's content-hash
        checksum) when ``id`` or ``updated_at`` is missing/null, rather than
        building an ambiguous checksum like ``"None:None"`` — every such row
        would otherwise collapse onto the same checksum and be treated as
        unchanged after the first one is seen, regardless of actual content.

        Caveat: even with both fields present, this assumes the source
        table's ``updated_at`` always changes whenever ``content`` changes.
        If a source table can update ``content`` without bumping
        ``updated_at`` (e.g. it's set by a trigger that's inconsistently
        applied), this connector will silently skip re-embedding a genuinely
        changed row. That's a real risk with this approach — the
        alternative, always returning ``None`` to force the base class's
        content-hash checksum, is always correct but re-hashes every row's
        content on every run. If your source table's ``updated_at`` isn't
        trustworthy, that tradeoff is worth making deliberately rather than
        assuming this checksum is safe by default.
        """
        row = item.source_ref
        row_id = row.get("id")
        updated_at = row.get("updated_at")
        if row_id is None or updated_at is None:
            return None
        return f"{row_id}:{updated_at}"

    # Fixed budget reserved for the id suffix in get_item_name(), so the
    # 255-char total is a real invariant regardless of what the row id looks
    # like (long string id, non-numeric id, etc.) rather than depending on
    # ids happening to be short. 32 hex chars covers a full MD5 digest.
    _ID_SUFFIX_MAX_LEN = 32

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe, unique name derived from the source
        name, row title, and row id.

        ``process_item`` keys the metadata tracker's dedup/versioning
        globally on this name (``MetadataTracker.get_latest_record()``
        filters only on this key, not per-connector-instance), so:

        - The source name is always included, so two configured database
          sources returning the same row id (e.g. ``id=1`` from both a
          ``postgres1`` and a ``mysql1`` source) don't collide on the same
          dedup/versioning key.
        - The row id is always included, so a title-only name would make two
          rows sharing the same title in the same source silently overwrite
          each other's embeddings.

        The source name and id are appended *after* truncating the title
        portion (rather than slugifying the whole combined string and
        truncating that), so a long title can never push them past the
        255-char limit and truncate them away.

        The id component is capped to ``_ID_SUFFIX_MAX_LEN`` chars and falls
        back to a ``h_``-prefixed full MD5 hash of the *raw* (un-slugified)
        id when either the slugified id would exceed that budget or the raw
        id contains characters ``slugify()`` normalizes away (e.g. ``"A/B"``
        and ``"A B"`` would otherwise both slugify to ``"A_B"`` and
        collide). The ``h_`` prefix ensures a hashed id can never collide
        with a literal id that happens to look like a hash.
        """
        row = item.source_ref
        raw_id = str(row.get("id", ""))
        slug_id = slugify(raw_id, max_len=self._ID_SUFFIX_MAX_LEN)
        if slug_id and slug_id == raw_id:
            row_id = slug_id
        elif raw_id:
            # Raw id doesn't survive slugify()/length round-trip as-is
            # (lossy normalization or too long) — use a stable hash of the
            # raw id instead so distinct ids can't collide on the suffix.
            row_id = "h_" + hashlib.md5(raw_id.encode("utf-8"), usedforsecurity=False).hexdigest()
        else:
            row_id = ""

        source = slugify(str(self.source_name or ""), max_len=64)
        title = str(row.get("title", "") or item.id)

        # Reserve room for "_<source>_<row_id>" so both always survive
        # truncation of the title portion.
        parts = [p for p in (source, row_id) if p]
        suffix = ("_" + "_".join(parts)) if parts else ""
        title_max_len = max(255 - len(suffix), 1)
        return (slugify(title, max_len=title_max_len) + suffix)[:255]

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
        """Best-effort check that the query's leading keyword is ``SELECT``
        or ``WITH``, so an obviously wrong config.query (e.g. one that
        starts with ``INSERT``/``UPDATE``/``DELETE``/DDL) fails fast at
        startup rather than at query time.

        This is NOT a SQL-injection guard and provides no real enforcement
        beyond that first token:

        - It does not stop stacked statements (e.g.
          ``SELECT 1; DROP TABLE x``).
        - Accepting ``WITH`` does not guarantee the query is read-only: a
          data-modifying CTE can still mutate data while the statement as a
          whole ends in a top-level SELECT, e.g.
          ``WITH deleted AS (DELETE FROM books RETURNING id, title,
          updated_at, content) SELECT * FROM deleted`` — this check has no
          way to see the DELETE inside the CTE body.

        This method intentionally does not attempt to parse CTE internals,
        comments, or full SQL grammar to close these gaps — the real
        safeguard against both is running this connector with read-only
        database credentials, so even a config.query that slips past this
        check (or is later edited to something destructive) cannot mutate
        data at the DB level.
        """
        stripped = re.sub(r"/\*.*?\*/", " ", query, flags=re.DOTALL)
        stripped = re.sub(r"--[^\n]*", " ", stripped)
        first_token = stripped.split()[0].upper() if stripped.split() else ""
        if first_token not in ("SELECT", "WITH"):
            raise ValueError(
                "config.query must be a SELECT statement (or a WITH ... SELECT CTE). "
                "This check only inspects the leading keyword — use read-only database "
                "credentials to actually enforce read-only access at the DB level."
            )
