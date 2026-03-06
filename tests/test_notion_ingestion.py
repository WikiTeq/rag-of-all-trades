import unittest
from unittest.mock import Mock, patch

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.notion_ingestion import NotionIngestionJob


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(
    integration_token="ntn_test",
    page_ids="",
    database_ids="",
    request_delay=0,
):
    return {
        "name": "test_notion",
        "config": {
            "integration_token": integration_token,
            "page_ids": page_ids,
            "database_ids": database_ids,
            "request_delay": request_delay,
        },
    }


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class TestNotionIngestionJob(unittest.TestCase):
    def setUp(self):
        self.reader_patcher = patch("tasks.notion_ingestion.NotionPageReader")
        self.mock_reader_class = self.reader_patcher.start()
        self.mock_reader = Mock()
        self.mock_reader_class.return_value = self.mock_reader

    def tearDown(self):
        self.reader_patcher.stop()

    def _make_job(self, **kwargs):
        return NotionIngestionJob(_make_config(**kwargs))

    # ------------------------------------------------------------------
    # Initialisation & validation
    # ------------------------------------------------------------------

    def test_source_type(self):
        job = self._make_job()
        self.assertEqual(job.source_type, "notion")

    def test_missing_integration_token_raises(self):
        with self.assertRaises(ValueError):
            NotionIngestionJob({"name": "x", "config": {}})

    def test_negative_request_delay_raises(self):
        with self.assertRaises(ValueError):
            self._make_job(request_delay=-1)

    def test_reader_initialized_with_token(self):
        self._make_job(integration_token="ntn_abc")
        self.mock_reader_class.assert_called_once_with(integration_token="ntn_abc")

    # ------------------------------------------------------------------
    # _parse_ids
    # ------------------------------------------------------------------

    def test_parse_ids_comma_separated_string(self):
        result = NotionIngestionJob._parse_ids("a, b, c")
        self.assertEqual(result, ["a", "b", "c"])

    def test_parse_ids_list(self):
        result = NotionIngestionJob._parse_ids(["a", "b"])
        self.assertEqual(result, ["a", "b"])

    def test_parse_ids_empty(self):
        self.assertEqual(NotionIngestionJob._parse_ids(""), [])
        self.assertEqual(NotionIngestionJob._parse_ids(None), [])

    # ------------------------------------------------------------------
    # list_items — selective mode (page_ids and/or database_ids)
    # ------------------------------------------------------------------

    def test_list_items_yields_configured_page_ids(self):
        job = self._make_job(page_ids="page-1,page-2")
        items = list(job.list_items())

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].id, "notion:page-1")
        self.assertEqual(items[1].id, "notion:page-2")
        self.mock_reader.list_pages.assert_not_called()
        self.mock_reader.list_databases.assert_not_called()

    def test_list_items_resolves_database_ids(self):
        self.mock_reader.query_database.return_value = ["db-page-1", "db-page-2"]
        job = self._make_job(database_ids="db-1")
        items = list(job.list_items())

        self.mock_reader.query_database.assert_called_once_with("db-1")
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].id, "notion:db-page-1")

    def test_list_items_combines_page_and_database_ids(self):
        self.mock_reader.query_database.return_value = ["db-page-1"]
        job = self._make_job(page_ids="page-1", database_ids="db-1")
        items = list(job.list_items())

        self.assertEqual(len(items), 2)
        ids = {item.id for item in items}
        self.assertIn("notion:page-1", ids)
        self.assertIn("notion:db-page-1", ids)

    def test_list_items_deduplicates_page_ids(self):
        self.mock_reader.query_database.return_value = ["page-1"]
        # page-1 appears in both page_ids and the database result
        job = self._make_job(page_ids="page-1", database_ids="db-1")
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "notion:page-1")

    def test_list_items_database_query_error_continues(self):
        self.mock_reader.query_database.side_effect = Exception("API error")
        job = self._make_job(database_ids="db-bad")
        items = list(job.list_items())
        self.assertEqual(items, [])

    # ------------------------------------------------------------------
    # list_items — load-all mode
    # ------------------------------------------------------------------

    def test_list_items_load_all_calls_list_pages_and_databases(self):
        self.mock_reader.list_pages.return_value = ["page-1"]
        self.mock_reader.list_databases.return_value = ["db-1"]
        self.mock_reader.query_database.return_value = ["db-page-1"]

        job = self._make_job()
        items = list(job.list_items())

        self.mock_reader.list_pages.assert_called_once()
        self.mock_reader.list_databases.assert_called_once()
        self.mock_reader.query_database.assert_called_once_with("db-1")
        ids = {item.id for item in items}
        self.assertIn("notion:page-1", ids)
        self.assertIn("notion:db-page-1", ids)

    def test_list_items_load_all_list_pages_error_continues(self):
        self.mock_reader.list_pages.side_effect = Exception("API error")
        self.mock_reader.list_databases.return_value = []
        job = self._make_job()
        items = list(job.list_items())
        self.assertEqual(items, [])

    # ------------------------------------------------------------------
    # get_raw_content
    # ------------------------------------------------------------------

    def test_get_raw_content_calls_reader_read_page(self):
        self.mock_reader.read_page.return_value = "Page content here"
        job = self._make_job()
        item = IngestionItem(id="notion:page-1", source_ref="page-1")

        content = job.get_raw_content(item)

        self.mock_reader.read_page.assert_called_once_with("page-1")
        self.assertEqual(content, "Page content here")

    def test_get_raw_content_returns_empty_string_on_error(self):
        self.mock_reader.read_page.side_effect = Exception("Not found")
        job = self._make_job()
        item = IngestionItem(id="notion:page-1", source_ref="page-1")

        content = job.get_raw_content(item)

        self.assertEqual(content, "")

    def test_get_raw_content_applies_request_delay(self):
        self.mock_reader.read_page.return_value = "text"
        job = self._make_job(request_delay=0.01)
        item = IngestionItem(id="notion:page-1", source_ref="page-1")

        with patch("tasks.notion_ingestion.time.sleep") as mock_sleep:
            job.get_raw_content(item)
            mock_sleep.assert_called_once_with(0.01)

    def test_get_raw_content_no_delay_when_zero(self):
        self.mock_reader.read_page.return_value = "text"
        job = self._make_job(request_delay=0)
        item = IngestionItem(id="notion:page-1", source_ref="page-1")

        with patch("tasks.notion_ingestion.time.sleep") as mock_sleep:
            job.get_raw_content(item)
            mock_sleep.assert_not_called()

    # ------------------------------------------------------------------
    # get_item_name
    # ------------------------------------------------------------------

    def test_get_item_name_returns_sanitized_page_id(self):
        job = self._make_job()
        item = IngestionItem(
            id="notion:abc-123", source_ref="abc-123"
        )
        name = job.get_item_name(item)
        self.assertEqual(name, "abc-123")

    def test_get_item_name_truncates_to_255(self):
        long_id = "a" * 300
        job = self._make_job()
        item = IngestionItem(id=f"notion:{long_id}", source_ref=long_id)
        name = job.get_item_name(item)
        self.assertLessEqual(len(name), 255)

    # ------------------------------------------------------------------
    # get_document_metadata
    # ------------------------------------------------------------------

    def test_get_document_metadata_contains_required_fields(self):
        job = self._make_job()
        item = IngestionItem(id="notion:abc-123", source_ref="abc-123")

        metadata = job.get_document_metadata(
            item=item,
            item_name="abc-123",
            checksum="chk",
            version=1,
            last_modified=None,
        )

        self.assertEqual(metadata["source"], "notion")
        self.assertEqual(metadata["id"], "abc-123")
        self.assertIn("notion.so", metadata["url"])
        self.assertEqual(metadata["key"], "abc-123")
        self.assertEqual(metadata["source_name"], "test_notion")

    def test_get_document_metadata_url_strips_dashes(self):
        job = self._make_job()
        item = IngestionItem(
            id="notion:12345678-1234-1234-1234-123456789abc",
            source_ref="12345678-1234-1234-1234-123456789abc",
        )
        metadata = job.get_document_metadata(
            item=item,
            item_name="test",
            checksum="chk",
            version=1,
            last_modified=None,
        )
        self.assertNotIn("-", metadata["url"].replace("https://notion.so/", ""))


if __name__ == "__main__":
    unittest.main()
