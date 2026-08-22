import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from tasks.database_ingestion import DatabaseIngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.parse import parse_list, parse_timestamp

BASE_CONFIG = {
    "name": "testdb",
    "config": {
        "type": "postgres",
        "connection_string": "postgresql+psycopg2://user:pass@localhost/db",
        "query": "SELECT id, title, updated_at, content FROM books",
    },
}

SAMPLE_ROWS = [
    {
        "id": 1,
        "title": "Book One",
        "updated_at": "2024-01-01T00:00:00",
        "content": "Content one",
        "author": "Alice",
        "year": 2020,
    },
    {
        "id": 2,
        "title": "Book Two",
        "updated_at": "2024-06-15T12:00:00",
        "content": "Content two",
        "author": "Bob",
        "year": 2021,
    },
]


def _make_job(config=None):
    return DatabaseIngestionJob(config or BASE_CONFIG)


class TestDatabaseIngestionJobInit(unittest.TestCase):
    def _job(self, overrides=None):
        config = {
            "name": "testdb",
            "config": {
                "type": "postgres",
                "connection_string": "postgresql+psycopg2://user:pass@localhost/db",
                "query": "SELECT id, title, updated_at, content FROM books",
                **(overrides or {}),
            },
        }
        return DatabaseIngestionJob(config)

    def test_source_type(self):
        self.assertEqual(self._job().source_type, "database")

    def test_valid_postgres(self):
        self.assertEqual(self._job({"type": "postgres"}).db_type, "postgres")

    def test_valid_mysql(self):
        self.assertEqual(self._job({"type": "mysql"}).db_type, "mysql")

    def test_invalid_type_raises(self):
        with self.assertRaises(ValueError):
            self._job({"type": "mssql"})

    def test_missing_type_raises(self):
        with self.assertRaises(ValueError):
            self._job({"type": ""})

    def test_missing_connection_string_raises(self):
        with self.assertRaises(ValueError):
            DatabaseIngestionJob({"name": "x", "config": {"type": "postgres", "query": "SELECT 1"}})

    def test_missing_query_raises(self):
        with self.assertRaises(ValueError):
            DatabaseIngestionJob(
                {"name": "x", "config": {"type": "postgres", "connection_string": "postgresql+psycopg2://x/y"}}
            )

    def test_non_select_query_raises(self):
        with self.assertRaises(ValueError):
            self._job({"query": "INSERT INTO books VALUES (1)"})

    def test_update_query_raises(self):
        with self.assertRaises(ValueError):
            self._job({"query": "UPDATE books SET title='x'"})

    def test_delete_query_raises(self):
        with self.assertRaises(ValueError):
            self._job({"query": "DELETE FROM books"})

    def test_ddl_query_raises(self):
        with self.assertRaises(ValueError):
            self._job({"query": "DROP TABLE books"})

    def test_select_with_leading_comment_allowed(self):
        job = self._job({"query": "/* comment */ SELECT id, title, updated_at, content FROM books"})
        self.assertIn("SELECT", job.query)

    def test_metadata_columns_from_string(self):
        job = self._job({"metadata_columns": "author, year, "})
        self.assertEqual(job.metadata_columns, ["author", "year"])

    def test_metadata_columns_from_list(self):
        job = self._job({"metadata_columns": ["author", "year"]})
        self.assertEqual(job.metadata_columns, ["author", "year"])

    def test_metadata_columns_empty(self):
        self.assertEqual(self._job().metadata_columns, [])


def _rows_generator(rows):
    """Wrap a list of rows in a real generator, matching _fetch_rows()'s
    actual return type (a generator, which supports .close())."""
    yield from rows


class TestDatabaseIngestionJobListItems(unittest.TestCase):
    def _job(self, rows=None, overrides=None):
        config = {
            "name": "testdb",
            "config": {
                "type": "postgres",
                "connection_string": "postgresql+psycopg2://user:pass@localhost/db",
                "query": "SELECT id, title, updated_at, content FROM books",
                **(overrides or {}),
            },
        }
        job = DatabaseIngestionJob(config)
        job._fetch_rows = MagicMock(side_effect=lambda: _rows_generator(rows if rows is not None else SAMPLE_ROWS))
        return job

    def test_yields_correct_count(self):
        self.assertEqual(len(list(self._job().list_items())), 2)

    def test_id_format(self):
        items = list(self._job().list_items())
        self.assertEqual(items[0].id, "database:testdb:1")
        self.assertEqual(items[1].id, "database:testdb:2")

    def test_source_ref_is_full_row(self):
        items = list(self._job().list_items())
        self.assertEqual(items[0].source_ref["title"], "Book One")

    def test_last_modified_parsed(self):
        items = list(self._job().list_items())
        self.assertIsInstance(items[0].last_modified, datetime)
        self.assertEqual(items[0].last_modified.year, 2024)

    def test_empty_result(self):
        self.assertEqual(list(self._job(rows=[]).list_items()), [])

    def test_fetch_error_raises(self):
        """_fetch_rows() is a generator: calling it never raises, only
        iterating it does (e.g. once execute() runs on first next()). Model
        that accurately rather than raising from the call itself."""

        def _raising_generator():
            raise Exception("connection failed")
            yield  # pragma: no cover - makes this a generator function

        job = self._job()
        job._fetch_rows = MagicMock(side_effect=_raising_generator)
        with self.assertRaises(Exception):
            list(job.list_items())


class TestDatabaseIngestionJobFetchRows(unittest.TestCase):
    """Tests for `_fetch_rows()`'s SQLAlchemy execution: streaming, column
    validation against the result's column names, and engine disposal."""

    def _job(self):
        config = {
            "name": "testdb",
            "config": {
                "type": "postgres",
                "connection_string": "postgresql+psycopg2://user:pass@localhost/db",
                "query": "SELECT id, title, updated_at, content FROM books",
            },
        }
        return DatabaseIngestionJob(config)

    def _mock_engine(self, keys, rows):
        """Build a create_engine() mock whose connect().execute() returns a
        mock SQLAlchemy Result with the given column names and row tuples."""
        result = MagicMock()
        result.keys.return_value = keys
        result.__iter__.return_value = iter(rows)

        conn = MagicMock()
        conn.execute.return_value = result
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False

        engine = MagicMock()
        engine.connect.return_value = conn
        return engine

    def test_streams_rows_without_fetchall(self):
        job = self._job()
        engine = self._mock_engine(
            keys=["id", "title", "updated_at", "content"],
            rows=[(1, "Book One", "2024-01-01", "Content one")],
        )
        with patch("tasks.database_ingestion.create_engine", return_value=engine):
            rows = list(job._fetch_rows())
        self.assertEqual(rows, [{"id": 1, "title": "Book One", "updated_at": "2024-01-01", "content": "Content one"}])
        engine.connect.return_value.execute.return_value.fetchall.assert_not_called()

    def test_missing_required_column_raises_on_empty_result(self):
        """An empty result set must still be validated against result.keys()."""
        job = self._job()
        engine = self._mock_engine(keys=["id", "title", "updated_at"], rows=[])  # missing content
        with patch("tasks.database_ingestion.create_engine", return_value=engine):
            with self.assertRaises(ValueError):
                list(job._fetch_rows())

    def test_missing_required_column_raises_with_rows(self):
        job = self._job()
        engine = self._mock_engine(keys=["id", "title", "updated_at"], rows=[(1, "X", None)])
        with patch("tasks.database_ingestion.create_engine", return_value=engine):
            with self.assertRaises(ValueError):
                list(job._fetch_rows())

    def test_engine_disposed_on_success(self):
        job = self._job()
        engine = self._mock_engine(
            keys=["id", "title", "updated_at", "content"],
            rows=[(1, "Book One", "2024-01-01", "Content one")],
        )
        with patch("tasks.database_ingestion.create_engine", return_value=engine):
            list(job._fetch_rows())
        engine.dispose.assert_called_once()

    def test_engine_disposed_on_error(self):
        job = self._job()
        engine = self._mock_engine(keys=["id"], rows=[])  # missing columns -> raises
        with patch("tasks.database_ingestion.create_engine", return_value=engine):
            with self.assertRaises(ValueError):
                list(job._fetch_rows())
        engine.dispose.assert_called_once()

    def test_engine_disposed_on_early_close(self):
        """Explicitly closing the _fetch_rows() generator (e.g. via GC or an
        explicit .close()) triggers its `finally: engine.dispose()`."""
        job = self._job()
        engine = self._mock_engine(
            keys=["id", "title", "updated_at", "content"],
            rows=[(1, "A", "2024-01-01", "x"), (2, "B", "2024-01-01", "y")],
        )
        with patch("tasks.database_ingestion.create_engine", return_value=engine):
            gen = job._fetch_rows()
            next(gen)
            gen.close()
        engine.dispose.assert_called_once()

    def test_list_items_disposes_engine_on_early_close(self):
        """The real caller path: closing job.list_items() itself (e.g. via an
        explicit .close(), or GC finalizing an abandoned generator) closes
        its inner _fetch_rows() generator in `finally`, disposing the engine
        promptly. Note this is distinct from a bare `break` out of a `for row
        in job.list_items(): ...` loop, which does not call .close() on the
        iterator per Python's iterator protocol."""
        job = self._job()
        engine = self._mock_engine(
            keys=["id", "title", "updated_at", "content"],
            rows=[(1, "A", "2024-01-01", "x"), (2, "B", "2024-01-01", "y")],
        )
        with patch("tasks.database_ingestion.create_engine", return_value=engine):
            gen = job.list_items()
            next(gen)
            gen.close()
        engine.dispose.assert_called_once()


class TestDatabaseIngestionJobContent(unittest.TestCase):
    def _job(self, metadata_columns="", name="testdb"):
        config = {
            "name": name,
            "config": {
                "type": "postgres",
                "connection_string": "postgresql+psycopg2://user:pass@localhost/db",
                "query": "SELECT id, title, updated_at, content FROM books",
                "metadata_columns": metadata_columns,
            },
        }
        return DatabaseIngestionJob(config)

    def _item(self, row):
        return IngestionItem(
            id=f"database:testdb:{row['id']}",
            source_ref=row,
            last_modified=None,
        )

    def test_get_raw_content(self):
        job = self._job()
        item = self._item(SAMPLE_ROWS[0])
        self.assertEqual(job.get_raw_content(item), "Content one")

    def test_get_raw_content_empty(self):
        job = self._job()
        item = self._item({"id": 1, "title": "T", "updated_at": None, "content": None})
        self.assertEqual(job.get_raw_content(item), "")

    def test_get_item_checksum_format(self):
        job = self._job()
        item = self._item(SAMPLE_ROWS[0])
        self.assertEqual(job.get_item_checksum(item), "1:2024-01-01T00:00:00")

    def test_get_item_checksum_changes_with_updated_at(self):
        job = self._job()
        row_a = {"id": 1, "title": "T", "updated_at": "2024-01-01", "content": "old"}
        row_b = {"id": 1, "title": "T", "updated_at": "2024-02-01", "content": "new"}
        self.assertNotEqual(job.get_item_checksum(self._item(row_a)), job.get_item_checksum(self._item(row_b)))

    def test_get_item_checksum_stable_when_unchanged(self):
        job = self._job()
        row = {"id": 1, "title": "T", "updated_at": "2024-01-01", "content": "same"}
        self.assertEqual(job.get_item_checksum(self._item(row)), job.get_item_checksum(self._item(dict(row))))

    def test_get_item_checksum_none_when_updated_at_missing(self):
        """A missing/null updated_at must fall back to None (base class's
        content-hash checksum), not build an ambiguous 'id:None' string that
        would collapse every such row onto the same checksum."""
        job = self._job()
        row = {"id": 1, "title": "T", "updated_at": None, "content": "x"}
        self.assertIsNone(job.get_item_checksum(self._item(row)))

    def test_get_item_checksum_none_when_id_missing(self):
        job = self._job()
        row = {"id": None, "title": "T", "updated_at": "2024-01-01", "content": "x"}
        self.assertIsNone(job.get_item_checksum(self._item(row)))

    def test_get_item_name_sanitizes(self):
        job = self._job()
        item = self._item({"id": 1, "title": "My Book: Vol. 1!", "updated_at": None, "content": ""})
        name = job.get_item_name(item)
        self.assertNotIn(":", name)
        self.assertNotIn("!", name)

    def test_get_item_name_includes_row_id(self):
        """Two rows with the same title must get distinct names, since
        process_item keys dedup/versioning on get_item_name()."""
        job = self._job()
        item1 = self._item({"id": 1, "title": "Same Title", "updated_at": None, "content": ""})
        item2 = self._item({"id": 2, "title": "Same Title", "updated_at": None, "content": ""})
        name1 = job.get_item_name(item1)
        name2 = job.get_item_name(item2)
        self.assertNotEqual(name1, name2)
        self.assertTrue(name1.endswith("_1"))
        self.assertTrue(name2.endswith("_2"))

    def test_get_item_name_truncates_but_preserves_id_suffix(self):
        """A long title must not push the row id past the 255-char limit and
        truncate it away — the id suffix must always survive."""
        job = self._job()
        item = self._item({"id": 12345, "title": "A" * 300, "updated_at": None, "content": ""})
        name = job.get_item_name(item)
        self.assertEqual(len(name), 255)
        self.assertTrue(name.endswith("_12345"), name)

    def test_get_item_name_stays_within_255_with_long_string_id(self):
        """The 255-char total must hold even when the id itself is long or
        non-numeric (e.g. a UUID or composite key), not just short int ids."""
        job = self._job()
        long_id = "x" * 300  # longer than _ID_SUFFIX_MAX_LEN even after slugifying
        item = self._item({"id": long_id, "title": "A" * 300, "updated_at": None, "content": ""})
        name = job.get_item_name(item)
        self.assertLessEqual(len(name), 255)

    def test_get_item_name_lossy_slugify_ids_do_not_collide(self):
        """Distinct ids that slugify() would normalize to the same string
        (e.g. 'A/B' and 'A B' both -> 'A_B') must still produce distinct
        item names, since get_item_name() is the dedup/versioning key."""
        job = self._job()
        item1 = self._item({"id": "A/B", "title": "Same Title", "updated_at": None, "content": ""})
        item2 = self._item({"id": "A B", "title": "Same Title", "updated_at": None, "content": ""})
        self.assertNotEqual(job.get_item_name(item1), job.get_item_name(item2))

    def test_get_item_name_includes_source_name(self):
        """MetadataTracker.get_latest_record() filters globally on this key,
        not per-connector-instance, so two configured database sources
        returning the same row id (e.g. id=1 from both a postgres1 and a
        mysql1 source) must not produce the same item name."""
        job_a = self._job(name="postgres1")
        job_b = self._job(name="mysql1")
        row = {"id": 1, "title": "Same Title", "updated_at": None, "content": ""}
        item_a = IngestionItem(id=f"database:postgres1:{row['id']}", source_ref=row, last_modified=None)
        item_b = IngestionItem(id=f"database:mysql1:{row['id']}", source_ref=row, last_modified=None)
        self.assertNotEqual(job_a.get_item_name(item_a), job_b.get_item_name(item_b))

    def test_get_item_name_falls_back_to_id(self):
        job = self._job()
        item = IngestionItem(
            id="database:testdb:99",
            source_ref={"id": 99, "title": "", "updated_at": None, "content": ""},
        )
        self.assertIn("testdb", job.get_item_name(item))
        self.assertTrue(job.get_item_name(item).endswith("_99"))

    def test_get_extra_metadata_base_fields(self):
        job = self._job()
        item = self._item(SAMPLE_ROWS[0])
        meta = job.get_extra_metadata(item, "", {})
        self.assertEqual(meta["title"], "Book One")
        self.assertEqual(meta["id"], "1")
        self.assertEqual(meta["db_type"], "postgres")

    def test_get_extra_metadata_extra_columns(self):
        job = self._job(metadata_columns="author,year")
        item = self._item(SAMPLE_ROWS[0])
        meta = job.get_extra_metadata(item, "", {})
        self.assertEqual(meta["author"], "Alice")
        self.assertEqual(meta["year"], 2020)

    def test_get_extra_metadata_skips_missing_extra_columns(self):
        job = self._job(metadata_columns="author,year")
        row = {"id": 1, "title": "T", "updated_at": None, "content": "C"}
        item = self._item(row)
        meta = job.get_extra_metadata(item, "", {})
        self.assertNotIn("author", meta)
        self.assertNotIn("year", meta)


class TestParseTimestamp(unittest.TestCase):
    def test_none_returns_none(self):
        self.assertIsNone(parse_timestamp(None))

    def test_datetime_passthrough(self):
        dt = datetime(2024, 1, 1, 12, 0)
        self.assertEqual(parse_timestamp(dt), dt)

    def test_iso_string(self):
        result = parse_timestamp("2024-06-15T12:00:00")
        self.assertIsInstance(result, datetime)
        self.assertEqual(result.month, 6)

    def test_invalid_string_returns_none(self):
        self.assertIsNone(parse_timestamp("not-a-date"))


class TestParseList(unittest.TestCase):
    def test_comma_string(self):
        self.assertEqual(parse_list("a, b, c"), ["a", "b", "c"])

    def test_list_input(self):
        self.assertEqual(parse_list(["a", "b"]), ["a", "b"])

    def test_empty_string(self):
        self.assertEqual(parse_list(""), [])

    def test_none(self):
        self.assertEqual(parse_list(None), [])

    def test_strips_empty_entries(self):
        self.assertEqual(parse_list("a,,b, "), ["a", "b"])
