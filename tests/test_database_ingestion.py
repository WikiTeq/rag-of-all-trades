import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from tasks.database_ingestion import DatabaseIngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

BASE_CONFIG = {
    "name": "testdb",
    "config": {
        "type": "postgres",
        "connection_string": "postgresql+psycopg://user:pass@localhost/db",
        "query": "SELECT id, title, updated_at, content FROM books",
    },
}

SAMPLE_ROWS = [
    {"id": 1, "title": "Book One", "updated_at": "2024-01-01T00:00:00", "content": "Content one", "author": "Alice", "year": 2020},
    {"id": 2, "title": "Book Two", "updated_at": "2024-06-15T12:00:00", "content": "Content two", "author": "Bob", "year": 2021},
]


def _make_job(config=None):
    cfg = config or BASE_CONFIG
    with patch.object(DatabaseIngestionJob, "_fetch_rows", return_value=[]):
        # patch _fetch_rows so __init__ doesn't open a real connection
        pass
    # DatabaseReader is instantiated in __init__ — patch it
    with patch("tasks.database_ingestion.DatabaseReader"):
        return DatabaseIngestionJob(cfg)


class TestDatabaseIngestionJobInit(unittest.TestCase):

    def _job(self, overrides=None):
        config = {
            "name": "testdb",
            "config": {
                "type": "postgres",
                "connection_string": "postgresql+psycopg://user:pass@localhost/db",
                "query": "SELECT id, title, updated_at, content FROM books",
                **(overrides or {}),
            },
        }
        with patch("tasks.database_ingestion.DatabaseReader"):
            return DatabaseIngestionJob(config)

    def test_source_type(self):
        self.assertEqual(self._job().source_type, "database")

    def test_valid_postgres(self):
        job = self._job({"type": "postgres"})
        self.assertEqual(job.db_type, "postgres")

    def test_valid_mysql(self):
        job = self._job({"type": "mysql"})
        self.assertEqual(job.db_type, "mysql")

    def test_invalid_type_raises(self):
        with self.assertRaises(ValueError, msg="type must be postgres or mysql"):
            self._job({"type": "mssql"})

    def test_missing_type_raises(self):
        with self.assertRaises(ValueError):
            self._job({"type": ""})

    def test_missing_connection_string_raises(self):
        config = {
            "name": "testdb",
            "config": {
                "type": "postgres",
                "query": "SELECT 1",
            },
        }
        with patch("tasks.database_ingestion.DatabaseReader"):
            with self.assertRaises(ValueError):
                DatabaseIngestionJob(config)

    def test_missing_query_raises(self):
        config = {
            "name": "testdb",
            "config": {
                "type": "postgres",
                "connection_string": "postgresql+psycopg://user:pass@localhost/db",
            },
        }
        with patch("tasks.database_ingestion.DatabaseReader"):
            with self.assertRaises(ValueError):
                DatabaseIngestionJob(config)

    def test_metadata_columns_from_string(self):
        job = self._job({"metadata_columns": "author, year, "})
        self.assertEqual(job.metadata_columns, ["author", "year"])

    def test_metadata_columns_from_list(self):
        job = self._job({"metadata_columns": ["author", "year"]})
        self.assertEqual(job.metadata_columns, ["author", "year"])

    def test_metadata_columns_empty(self):
        job = self._job()
        self.assertEqual(job.metadata_columns, [])

    def test_required_columns_default(self):
        job = self._job()
        self.assertEqual(job.required_columns, set())

    def test_required_columns_custom(self):
        job = self._job({"required_columns": "id,body,created_at"})
        self.assertEqual(job.required_columns, {"id", "body", "created_at"})


class TestDatabaseIngestionJobListItems(unittest.TestCase):

    def _job(self, rows=None, metadata_columns=""):
        config = {
            "name": "testdb",
            "config": {
                "type": "postgres",
                "connection_string": "postgresql+psycopg://user:pass@localhost/db",
                "query": "SELECT id, title, updated_at, content FROM books",
                "metadata_columns": metadata_columns,
            },
        }
        with patch("tasks.database_ingestion.DatabaseReader"):
            job = DatabaseIngestionJob(config)
        job._fetch_rows = MagicMock(return_value=rows if rows is not None else SAMPLE_ROWS)
        return job

    def test_list_items_yields_correct_count(self):
        job = self._job()
        items = list(job.list_items())
        self.assertEqual(len(items), 2)

    def test_list_items_id_format(self):
        job = self._job()
        items = list(job.list_items())
        self.assertEqual(items[0].id, "database:testdb:1")
        self.assertEqual(items[1].id, "database:testdb:2")

    def test_list_items_source_ref_is_row(self):
        job = self._job()
        items = list(job.list_items())
        self.assertEqual(items[0].source_ref["title"], "Book One")

    def test_list_items_last_modified_parsed(self):
        job = self._job()
        items = list(job.list_items())
        self.assertIsInstance(items[0].last_modified, datetime)
        self.assertEqual(items[0].last_modified.year, 2024)

    def test_list_items_empty_result(self):
        job = self._job(rows=[])
        items = list(job.list_items())
        self.assertEqual(items, [])

    def test_list_items_missing_required_column_raises(self):
        bad_rows = [{"id": 1, "title": "X", "updated_at": None}]  # missing 'content'
        config = {
            "name": "testdb",
            "config": {
                "type": "postgres",
                "connection_string": "postgresql+psycopg://user:pass@localhost/db",
                "query": "SELECT id, title, updated_at, content FROM books",
                "required_columns": "id,title,updated_at,content",
            },
        }
        with patch("tasks.database_ingestion.DatabaseReader"):
            job = DatabaseIngestionJob(config)
        job._fetch_rows = MagicMock(return_value=bad_rows)
        with self.assertRaises(ValueError, msg="missing required columns"):
            list(job.list_items())

    def test_list_items_fetch_error_logs_and_returns_empty(self):
        job = self._job()
        job._fetch_rows = MagicMock(side_effect=Exception("connection failed"))
        items = list(job.list_items())
        self.assertEqual(items, [])


class TestDatabaseIngestionJobContent(unittest.TestCase):

    def _job(self):
        config = {
            "name": "testdb",
            "config": {
                "type": "postgres",
                "connection_string": "postgresql+psycopg://user:pass@localhost/db",
                "query": "SELECT id, title, updated_at, content FROM books",
                "metadata_columns": "author,year",
            },
        }
        with patch("tasks.database_ingestion.DatabaseReader"):
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

    def test_get_item_name_sanitizes(self):
        job = self._job()
        item = self._item({"id": 1, "title": "My Book: Vol. 1!", "updated_at": None, "content": ""})
        name = job.get_item_name(item)
        self.assertNotIn(":", name)
        self.assertNotIn("!", name)

    def test_get_item_name_truncates(self):
        job = self._job()
        item = self._item({"id": 1, "title": "A" * 300, "updated_at": None, "content": ""})
        self.assertEqual(len(job.get_item_name(item)), 255)

    def test_get_item_name_falls_back_to_id(self):
        job = self._job()
        item = IngestionItem(
            id="database:testdb:99",
            source_ref={"id": 99, "title": "", "updated_at": None, "content": ""},
        )
        name = job.get_item_name(item)
        self.assertIn("testdb", name)

    def test_get_document_metadata_base_fields(self):
        job = self._job()
        item = self._item(SAMPLE_ROWS[0])
        with patch.object(job.__class__.__bases__[0], "get_document_metadata", return_value={}):
            meta = job.get_document_metadata(item, "book_one", "abc123", 1, None)
        self.assertEqual(meta["title"], "Book One")
        self.assertEqual(meta["id"], "1")
        self.assertEqual(meta["db_type"], "postgres")

    def test_get_document_metadata_extra_columns(self):
        job = self._job()
        item = self._item(SAMPLE_ROWS[0])
        with patch.object(job.__class__.__bases__[0], "get_document_metadata", return_value={}):
            meta = job.get_document_metadata(item, "book_one", "abc123", 1, None)
        self.assertEqual(meta["author"], "Alice")
        self.assertEqual(meta["year"], 2020)

    def test_get_document_metadata_skips_missing_extra_columns(self):
        job = self._job()
        row = {"id": 1, "title": "T", "updated_at": None, "content": "C"}  # no author/year
        item = self._item(row)
        with patch.object(job.__class__.__bases__[0], "get_document_metadata", return_value={}):
            meta = job.get_document_metadata(item, "t", "abc", 1, None)
        self.assertNotIn("author", meta)
        self.assertNotIn("year", meta)


class TestDatabaseIngestionJobParseTimestamp(unittest.TestCase):

    def test_none_returns_none(self):
        self.assertIsNone(DatabaseIngestionJob._parse_timestamp(None))

    def test_datetime_passthrough(self):
        dt = datetime(2024, 1, 1, 12, 0)
        self.assertEqual(DatabaseIngestionJob._parse_timestamp(dt), dt)

    def test_iso_string(self):
        result = DatabaseIngestionJob._parse_timestamp("2024-06-15T12:00:00")
        self.assertIsInstance(result, datetime)
        self.assertEqual(result.month, 6)

    def test_invalid_string_returns_none(self):
        self.assertIsNone(DatabaseIngestionJob._parse_timestamp("not-a-date"))


class TestDatabaseIngestionJobParseList(unittest.TestCase):

    def test_comma_string(self):
        self.assertEqual(DatabaseIngestionJob._parse_list("a, b, c"), ["a", "b", "c"])

    def test_list_input(self):
        self.assertEqual(DatabaseIngestionJob._parse_list(["a", "b"]), ["a", "b"])

    def test_empty_string(self):
        self.assertEqual(DatabaseIngestionJob._parse_list(""), [])

    def test_none(self):
        self.assertEqual(DatabaseIngestionJob._parse_list(None), [])

    def test_strips_empty_entries(self):
        self.assertEqual(DatabaseIngestionJob._parse_list("a,,b, "), ["a", "b"])
