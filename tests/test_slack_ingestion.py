import unittest
from datetime import datetime
from unittest.mock import Mock, patch

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.slack_ingestion import SlackIngestionJob

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(
    token="xoxb-test-token",
    channel_ids="",
    channel_patterns="",
    channel_types="public_channel,private_channel",
    earliest_date="",
    latest_date="",
):
    return {
        "name": "test_slack",
        "config": {
            "token": token,
            "channel_ids": channel_ids,
            "channel_patterns": channel_patterns,
            "channel_types": channel_types,
            "earliest_date": earliest_date,
            "latest_date": latest_date,
        },
    }


def _make_doc(text="Hello from Slack"):
    doc = Mock()
    doc.text = text
    return doc


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class TestSlackIngestionJob(unittest.TestCase):
    def setUp(self):
        self.reader_patcher = patch("tasks.slack_ingestion.SlackReader")
        self.mock_reader_class = self.reader_patcher.start()
        self.mock_reader = Mock()
        self.mock_reader_class.return_value = self.mock_reader

    def tearDown(self):
        self.reader_patcher.stop()

    def _make_job(self, **kwargs):
        return SlackIngestionJob(_make_config(**kwargs))

    # ------------------------------------------------------------------
    # source_type
    # ------------------------------------------------------------------

    def test_source_type(self):
        job = self._make_job(channel_ids="C123")
        self.assertEqual(job.source_type, "slack")

    # ------------------------------------------------------------------
    # Initialisation & validation
    # ------------------------------------------------------------------

    def test_missing_token_raises(self):
        with self.assertRaises(ValueError):
            SlackIngestionJob({"name": "x", "config": {}})

    def test_blank_token_raises(self):
        with self.assertRaises(ValueError):
            SlackIngestionJob({"name": "x", "config": {"token": "   "}})

    def test_channel_ids_and_channel_patterns_mutually_exclusive(self):
        with self.assertRaises(ValueError):
            self._make_job(channel_ids="C123", channel_patterns="general")

    def test_latest_date_without_earliest_date_raises(self):
        with self.assertRaises(ValueError):
            self._make_job(latest_date="2025-01-01")

    def test_valid_date_range_does_not_raise(self):
        job = self._make_job(
            channel_ids="C123",
            earliest_date="2024-01-01",
            latest_date="2025-01-01",
        )
        self.assertIsInstance(job.earliest_date, datetime)
        self.assertIsInstance(job.latest_date, datetime)

    def test_only_earliest_date_is_valid(self):
        job = self._make_job(channel_ids="C123", earliest_date="2024-01-01")
        self.assertIsInstance(job.earliest_date, datetime)
        self.assertIsNone(job.latest_date)

    def test_no_dates_is_valid(self):
        job = self._make_job(channel_ids="C123")
        self.assertIsNone(job.earliest_date)
        self.assertIsNone(job.latest_date)

    def test_reader_initialized_with_token_and_channel_types(self):
        self._make_job(channel_ids="C123")
        self.mock_reader_class.assert_called_once()
        call_kwargs = self.mock_reader_class.call_args.kwargs
        self.assertEqual(call_kwargs["slack_token"], "xoxb-test-token")
        self.assertEqual(
            call_kwargs["channel_types"], "public_channel,private_channel"
        )

    def test_custom_channel_types_passed_to_reader(self):
        self._make_job(channel_ids="C123", channel_types="public_channel")
        call_kwargs = self.mock_reader_class.call_args.kwargs
        self.assertEqual(call_kwargs["channel_types"], "public_channel")

    # ------------------------------------------------------------------
    # _parse_ids
    # ------------------------------------------------------------------

    def test_parse_ids_comma_string(self):
        result = SlackIngestionJob._parse_ids("C111,C222, C333")
        self.assertEqual(result, ["C111", "C222", "C333"])

    def test_parse_ids_list(self):
        result = SlackIngestionJob._parse_ids(["C111", "C222"])
        self.assertEqual(result, ["C111", "C222"])

    def test_parse_ids_empty_string(self):
        self.assertEqual(SlackIngestionJob._parse_ids(""), [])

    def test_parse_ids_none(self):
        self.assertEqual(SlackIngestionJob._parse_ids(None), [])

    def test_parse_ids_filters_blank_entries(self):
        result = SlackIngestionJob._parse_ids("C111,,  ,C222")
        self.assertEqual(result, ["C111", "C222"])

    # ------------------------------------------------------------------
    # _parse_date
    # ------------------------------------------------------------------

    def test_parse_date_valid(self):
        result = SlackIngestionJob._parse_date("2024-06-15")
        self.assertEqual(result, datetime(2024, 6, 15))

    def test_parse_date_invalid_raises(self):
        with self.assertRaises(ValueError):
            SlackIngestionJob._parse_date("15-06-2024")

    def test_parse_date_non_date_string_raises(self):
        with self.assertRaises(ValueError):
            SlackIngestionJob._parse_date("not-a-date")

    # ------------------------------------------------------------------
    # list_items — channel_ids mode
    # ------------------------------------------------------------------

    def test_list_items_with_explicit_channel_ids(self):
        job = self._make_job(channel_ids="C111,C222")
        items = list(job.list_items())

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].id, "slack:C111")
        self.assertEqual(items[0].source_ref, "C111")
        self.assertEqual(items[1].id, "slack:C222")
        self.assertIsNone(items[0].last_modified)

    def test_list_items_channel_ids_as_list(self):
        config = {
            "name": "test_slack",
            "config": {
                "token": "xoxb-test",
                "channel_ids": ["C111", "C222"],
            },
        }
        job = SlackIngestionJob(config)
        items = list(job.list_items())
        self.assertEqual(len(items), 2)

    # ------------------------------------------------------------------
    # list_items — channel_patterns mode
    # ------------------------------------------------------------------

    def test_list_items_with_channel_patterns(self):
        self.mock_reader.get_channel_ids.return_value = ["C001", "C002", "C003"]

        job = self._make_job(channel_patterns="general,eng.*")
        items = list(job.list_items())

        self.assertEqual(len(items), 3)
        self.mock_reader.get_channel_ids.assert_called_once_with(
            channel_patterns=["general", "eng.*"]
        )

    def test_list_items_pattern_resolution_failure_returns_empty(self):
        self.mock_reader.get_channel_ids.side_effect = Exception("API error")

        job = self._make_job(channel_patterns="general")
        items = list(job.list_items())

        self.assertEqual(items, [])

    # ------------------------------------------------------------------
    # list_items — no channels configured
    # ------------------------------------------------------------------

    def test_list_items_no_channels_configured_returns_empty(self):
        job = self._make_job()
        items = list(job.list_items())
        self.assertEqual(items, [])

    # ------------------------------------------------------------------
    # get_raw_content
    # ------------------------------------------------------------------

    def test_get_raw_content_returns_doc_text(self):
        self.mock_reader.load_data.return_value = [_make_doc("Hello world")]

        job = self._make_job(channel_ids="C123")
        item = IngestionItem(id="slack:C123", source_ref="C123")
        content = job.get_raw_content(item)

        self.assertEqual(content, "Hello world")
        self.mock_reader.load_data.assert_called_once_with(channel_ids=["C123"])

    def test_get_raw_content_empty_docs_returns_empty_string(self):
        self.mock_reader.load_data.return_value = []

        job = self._make_job(channel_ids="C123")
        item = IngestionItem(id="slack:C123", source_ref="C123")
        content = job.get_raw_content(item)

        self.assertEqual(content, "")

    def test_get_raw_content_none_text_returns_empty_string(self):
        doc = Mock()
        doc.text = None
        self.mock_reader.load_data.return_value = [doc]

        job = self._make_job(channel_ids="C123")
        item = IngestionItem(id="slack:C123", source_ref="C123")
        content = job.get_raw_content(item)

        self.assertEqual(content, "")

    def test_get_raw_content_reader_exception_returns_empty_string(self):
        self.mock_reader.load_data.side_effect = Exception("Rate limit")

        job = self._make_job(channel_ids="C123")
        item = IngestionItem(id="slack:C123", source_ref="C123")
        content = job.get_raw_content(item)

        self.assertEqual(content, "")

    # ------------------------------------------------------------------
    # get_item_name
    # ------------------------------------------------------------------

    def test_get_item_name_returns_sanitized_channel_id(self):
        job = self._make_job(channel_ids="C123456")
        item = IngestionItem(id="slack:C123456", source_ref="C123456")
        self.assertEqual(job.get_item_name(item), "C123456")

    def test_get_item_name_sanitizes_special_chars(self):
        job = self._make_job(channel_ids="C123")
        item = IngestionItem(id="slack:C123", source_ref="C!@#channel")
        name = job.get_item_name(item)
        self.assertNotIn("!", name)
        self.assertNotIn("@", name)
        self.assertNotIn("#", name)

    def test_get_item_name_truncates_to_255(self):
        long_id = "C" + "x" * 300
        job = self._make_job(channel_ids=long_id)
        item = IngestionItem(id=f"slack:{long_id}", source_ref=long_id)
        self.assertLessEqual(len(job.get_item_name(item)), 255)

    # ------------------------------------------------------------------
    # get_document_metadata
    # ------------------------------------------------------------------

    def test_get_document_metadata_contains_channel_id_and_url(self):
        job = self._make_job(channel_ids="C123456")
        item = IngestionItem(id="slack:C123456", source_ref="C123456")
        metadata = job.get_document_metadata(
            item=item,
            item_name="C123456",
            checksum="abc123",
            version=1,
            last_modified=None,
        )

        self.assertEqual(metadata["channel_id"], "C123456")
        self.assertEqual(
            metadata["url"],
            "https://slack.com/app_redirect?channel=C123456",
        )

    def test_get_document_metadata_base_fields_present(self):
        job = self._make_job(channel_ids="C123456")
        item = IngestionItem(id="slack:C123456", source_ref="C123456")
        metadata = job.get_document_metadata(
            item=item,
            item_name="C123456",
            checksum="abc123",
            version=2,
            last_modified=datetime(2024, 6, 1),
        )

        self.assertEqual(metadata["source"], "slack")
        self.assertEqual(metadata["source_name"], "test_slack")
        self.assertEqual(metadata["checksum"], "abc123")
        self.assertEqual(metadata["version"], 2)

    # ------------------------------------------------------------------
    # Integration: process_item
    # ------------------------------------------------------------------

    def test_process_item_calls_vector_store_and_metadata_tracker(self):
        self.mock_reader.load_data.return_value = [_make_doc("channel content")]

        job = self._make_job(channel_ids="C123")
        item = IngestionItem(id="slack:C123", source_ref="C123")
        job.vector_manager.insert_documents = Mock()

        with (
            patch.object(
                job.metadata_tracker, "get_latest_record", return_value=None
            ),
            patch.object(
                job.metadata_tracker, "record_metadata"
            ) as mock_record,
            patch.object(job.metadata_tracker, "delete_previous_embeddings"),
        ):
            result = job.process_item(item)

            self.assertEqual(result, 1)
            job.vector_manager.insert_documents.assert_called_once()
            mock_record.assert_called_once()

    def test_process_item_skips_duplicate_checksum(self):
        self.mock_reader.load_data.return_value = [_make_doc("same content")]

        job = self._make_job(channel_ids="C123")
        item = IngestionItem(id="slack:C123", source_ref="C123")
        job._seen_add = Mock(return_value=False)

        with patch.object(
            job.metadata_tracker, "get_latest_record", return_value=None
        ):
            with patch.object(job.metadata_tracker, "record_metadata"):
                job.vector_manager.insert_documents = Mock()
                result = job.process_item(item)

        self.assertEqual(result, 0)
        job.vector_manager.insert_documents.assert_not_called()


if __name__ == "__main__":
    unittest.main()
