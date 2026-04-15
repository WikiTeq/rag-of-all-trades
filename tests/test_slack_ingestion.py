import unittest
from datetime import UTC, datetime
from unittest.mock import MagicMock, Mock, patch

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


def _make_message(ts="1700000000.000001", text="Hello from Slack"):
    return {"ts": ts, "text": text}


def _make_history_result(messages, has_more=False):
    result = {"messages": messages, "has_more": has_more}
    if has_more:
        result["response_metadata"] = {"next_cursor": "next123"}
    return result


def _make_replies_result(messages, has_more=False):
    result = {"messages": messages, "has_more": has_more}
    if has_more:
        result["response_metadata"] = {"next_cursor": "next456"}
    return result


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class TestSlackIngestionJob(unittest.TestCase):
    def setUp(self):
        self.webclient_patcher = patch("tasks.slack_ingestion.WebClient")
        self.mock_webclient_class = self.webclient_patcher.start()
        self.mock_client = MagicMock()
        self.mock_webclient_class.return_value = self.mock_client

    def tearDown(self):
        self.webclient_patcher.stop()

    def _make_job(self, **kwargs):
        return SlackIngestionJob(_make_config(**kwargs))

    def _setup_client(self, history_messages=None, replies_messages=None):
        """Wire up mock responses on the client."""
        if history_messages is not None:
            self.mock_client.conversations_history.return_value = (
                _make_history_result(history_messages)
            )
        if replies_messages is not None:
            self.mock_client.conversations_replies.return_value = (
                _make_replies_result(replies_messages)
            )
        return self.mock_client

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

    def test_inverted_date_range_raises(self):
        with self.assertRaises(ValueError):
            self._make_job(
                channel_ids="C123",
                earliest_date="2025-01-01",
                latest_date="2024-01-01",
            )

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

    def test_webclient_initialized_with_token(self):
        self._make_job(channel_ids="C123")
        self.mock_webclient_class.assert_called_once_with(
            token="xoxb-test-token"
        )

    def test_custom_channel_types_stored(self):
        job = self._make_job(
            channel_ids="C123", channel_types="public_channel"
        )
        self.assertEqual(job.channel_types, "public_channel")

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
        self.assertEqual(result, datetime(2024, 6, 15, tzinfo=UTC))

    def test_parse_date_invalid_raises(self):
        with self.assertRaises(ValueError):
            SlackIngestionJob._parse_date("15-06-2024")

    def test_parse_date_non_date_string_raises(self):
        with self.assertRaises(ValueError):
            SlackIngestionJob._parse_date("not-a-date")

    # ------------------------------------------------------------------
    # list_items — channel_ids mode, yields per message
    # ------------------------------------------------------------------

    def test_list_items_yields_one_item_per_message(self):
        job = self._make_job(channel_ids="C111")
        mock_client = self._setup_client(
            history_messages=[
                _make_message("1700000001.000001", "msg1"),
                _make_message("1700000002.000002", "msg2"),
            ],
            replies_messages=[],
        )
        mock_client.conversations_replies.return_value = _make_replies_result(
            [_make_message("1700000001.000001", "msg1")]
        )

        items = list(job.list_items())
        self.assertEqual(len(items), 2)

    def test_list_items_item_id_format(self):
        job = self._make_job(channel_ids="C111")
        mock_client = self._setup_client()
        mock_client.conversations_history.return_value = _make_history_result(
            [_make_message("1700000001.000001", "msg")]
        )
        mock_client.conversations_replies.return_value = _make_replies_result(
            [_make_message("1700000001.000001", "msg")]
        )

        items = list(job.list_items())
        self.assertEqual(
            items[0].id, "slack:test_slack:C111:1700000001.000001"
        )

    def test_list_items_source_ref_contains_channel_ts_text(self):
        job = self._make_job(channel_ids="C111")
        mock_client = self._setup_client()
        mock_client.conversations_history.return_value = _make_history_result(
            [_make_message("1700000001.000001", "hello")]
        )
        mock_client.conversations_replies.return_value = _make_replies_result(
            [_make_message("1700000001.000001", "hello")]
        )

        items = list(job.list_items())
        ref = items[0].source_ref
        self.assertEqual(ref["channel_id"], "C111")
        self.assertEqual(ref["message_ts"], "1700000001.000001")
        self.assertIn("hello", ref["text"])

    def test_list_items_last_modified_parsed_from_ts(self):
        job = self._make_job(channel_ids="C111")
        mock_client = self._setup_client()
        ts = "1700000001.000001"
        mock_client.conversations_history.return_value = _make_history_result(
            [_make_message(ts, "msg")]
        )
        mock_client.conversations_replies.return_value = _make_replies_result(
            [_make_message(ts, "msg")]
        )

        items = list(job.list_items())
        self.assertIsInstance(items[0].last_modified, datetime)
        self.assertAlmostEqual(
            items[0].last_modified.timestamp(), float(ts), places=0
        )

    def test_list_items_empty_channel_yields_nothing(self):
        job = self._make_job(channel_ids="C111")
        mock_client = self._setup_client()
        mock_client.conversations_history.return_value = _make_history_result(
            []
        )

        items = list(job.list_items())
        self.assertEqual(items, [])

    def test_list_items_multiple_channels(self):
        job = self._make_job(channel_ids="C111,C222")
        mock_client = self._setup_client()
        mock_client.conversations_history.return_value = _make_history_result(
            [_make_message("1700000001.000001", "msg")]
        )
        mock_client.conversations_replies.return_value = _make_replies_result(
            [_make_message("1700000001.000001", "msg")]
        )

        items = list(job.list_items())
        self.assertEqual(len(items), 2)

    # ------------------------------------------------------------------
    # list_items — channel_patterns mode
    # ------------------------------------------------------------------

    def test_list_items_with_channel_patterns(self):
        self.mock_client.conversations_list.return_value = {
            "channels": [
                {"id": "C001", "name": "general"},
                {"id": "C002", "name": "eng-backend"},
            ]
        }
        self.mock_client.conversations_history.return_value = (
            _make_history_result([_make_message("1700000001.000001", "msg")])
        )
        self.mock_client.conversations_replies.return_value = (
            _make_replies_result([_make_message("1700000001.000001", "msg")])
        )

        job = self._make_job(channel_patterns="general,eng.*")
        items = list(job.list_items())
        self.assertEqual(len(items), 2)
        self.mock_client.conversations_list.assert_called_once_with(
            types="public_channel,private_channel"
        )

    def test_list_items_pattern_resolution_failure_returns_empty(self):
        self.mock_client.conversations_list.side_effect = Exception(
            "API error"
        )

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

    def test_get_raw_content_returns_text_from_source_ref(self):
        job = self._make_job(channel_ids="C123")
        item = IngestionItem(
            id="slack:test_slack:C123:1700000001.000001",
            source_ref={
                "channel_id": "C123",
                "message_ts": "1700000001.000001",
                "text": "Hello world",
            },
        )
        self.assertEqual(job.get_raw_content(item), "Hello world")

    def test_get_raw_content_empty_text_returns_empty_string(self):
        job = self._make_job(channel_ids="C123")
        item = IngestionItem(
            id="slack:test_slack:C123:1700000001.000001",
            source_ref={
                "channel_id": "C123",
                "message_ts": "1700000001.000001",
                "text": "",
            },
        )
        self.assertEqual(job.get_raw_content(item), "")

    def test_get_raw_content_none_text_returns_empty_string(self):
        job = self._make_job(channel_ids="C123")
        item = IngestionItem(
            id="slack:test_slack:C123:1700000001.000001",
            source_ref={
                "channel_id": "C123",
                "message_ts": "1700000001.000001",
                "text": None,
            },
        )
        self.assertEqual(job.get_raw_content(item), "")

    # ------------------------------------------------------------------
    # get_item_name
    # ------------------------------------------------------------------

    def test_get_item_name_format(self):
        job = self._make_job(channel_ids="C123456")
        item = IngestionItem(
            id="slack:test_slack:C123456:1700000001.000001",
            source_ref={
                "channel_id": "C123456",
                "message_ts": "1700000001.000001",
                "text": "",
            },
        )
        name = job.get_item_name(item)
        self.assertIn("C123456", name)
        self.assertIn("1700000001_000001", name)

    def test_get_item_name_sanitizes_special_chars(self):
        job = self._make_job(channel_ids="C123")
        item = IngestionItem(
            id="slack:test_slack:C123:1700000001.000001",
            source_ref={
                "channel_id": "C123",
                "message_ts": "1700000001.000001",
                "text": "",
            },
        )
        name = job.get_item_name(item)
        self.assertNotIn(".", name)

    def test_get_item_name_truncates_to_255(self):
        long_id = "C" + "x" * 300
        job = self._make_job(channel_ids="C123")
        item = IngestionItem(
            id="slack:test_slack:C123:1700000001.000001",
            source_ref={
                "channel_id": long_id,
                "message_ts": "1700000001.000001",
                "text": "",
            },
        )
        self.assertLessEqual(len(job.get_item_name(item)), 255)

    # ------------------------------------------------------------------
    # get_document_metadata
    # ------------------------------------------------------------------

    def test_get_document_metadata_contains_channel_id_ts_and_url(self):
        job = self._make_job(channel_ids="C123456")
        item = IngestionItem(
            id="slack:test_slack:C123456:1700000001.000001",
            source_ref={
                "channel_id": "C123456",
                "message_ts": "1700000001.000001",
                "text": "msg",
            },
        )
        metadata = job.get_document_metadata(
            item=item,
            item_name="C123456_1700000001_000001",
            checksum="abc123",
            version=1,
            last_modified=None,
        )

        self.assertEqual(metadata["channel_id"], "C123456")
        self.assertEqual(metadata["message_ts"], "1700000001.000001")
        self.assertIn("C123456", metadata["url"])

    # ------------------------------------------------------------------
    # _get_channel_ids_by_patterns
    # ------------------------------------------------------------------

    def test_get_channel_ids_by_patterns_exact_match(self):
        job = self._make_job(channel_patterns="general")
        self.mock_client.conversations_list.return_value = {
            "channels": [
                {"id": "C001", "name": "general"},
                {"id": "C002", "name": "random"},
            ]
        }
        ids = job._get_channel_ids_by_patterns(["general"])
        self.assertEqual(ids, ["C001"])

    def test_get_channel_ids_by_patterns_regex_match(self):
        job = self._make_job(channel_patterns="eng.*")
        self.mock_client.conversations_list.return_value = {
            "channels": [
                {"id": "C001", "name": "eng-backend"},
                {"id": "C002", "name": "eng-frontend"},
                {"id": "C003", "name": "random"},
            ]
        }
        ids = sorted(job._get_channel_ids_by_patterns(["eng.*"]))
        self.assertEqual(ids, ["C001", "C002"])

    def test_get_channel_ids_by_patterns_no_match(self):
        job = self._make_job(channel_patterns="nonexistent")
        self.mock_client.conversations_list.return_value = {
            "channels": [{"id": "C001", "name": "general"}]
        }
        ids = job._get_channel_ids_by_patterns(["nonexistent"])
        self.assertEqual(ids, [])

    def test_get_channel_ids_by_patterns_paginates(self):
        job = self._make_job(channel_patterns="general")
        self.mock_client.conversations_list.side_effect = [
            {
                "channels": [{"id": "C001", "name": "general"}],
                "response_metadata": {"next_cursor": "cursor1"},
            },
            {
                "channels": [{"id": "C002", "name": "general-2"}],
                "response_metadata": {"next_cursor": ""},
            },
        ]
        ids = job._get_channel_ids_by_patterns(["general"])
        self.assertEqual(ids, ["C001"])
        self.assertEqual(self.mock_client.conversations_list.call_count, 2)

    def test_get_channel_ids_by_patterns_preserves_discovery_order(self):
        job = self._make_job(channel_patterns="eng.*")
        self.mock_client.conversations_list.return_value = {
            "channels": [
                {"id": "C001", "name": "eng-backend"},
                {"id": "C002", "name": "eng-frontend"},
                {"id": "C001", "name": "eng-backend"},  # duplicate
            ]
        }
        ids = job._get_channel_ids_by_patterns(["eng.*"])
        self.assertEqual(ids, ["C001", "C002"])

    # ------------------------------------------------------------------
    # _yield_messages — thread broadcast / threadless skip
    # ------------------------------------------------------------------

    def test_yield_messages_skips_thread_broadcasts(self):
        job = self._make_job(channel_ids="C123")
        self.mock_client.conversations_history.return_value = _make_history_result(
            [
                _make_message("1700000001.000001", "top-level msg"),
                {
                    "ts": "1700000001.000002",
                    "text": "broadcast",
                    "subtype": "thread_broadcast",
                    "thread_ts": "1700000001.000001",
                },
            ]
        )
        self.mock_client.conversations_replies.return_value = _make_replies_result(
            [_make_message("1700000001.000001", "top-level msg")]
        )
        items = list(job.list_items())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].source_ref["message_ts"], "1700000001.000001")

    def test_yield_messages_skips_thread_replies_in_channel(self):
        job = self._make_job(channel_ids="C123")
        self.mock_client.conversations_history.return_value = _make_history_result(
            [
                _make_message("1700000001.000001", "top-level msg"),
                {
                    "ts": "1700000001.000002",
                    "text": "reply",
                    "thread_ts": "1700000001.000001",  # different from ts → reply
                },
            ]
        )
        self.mock_client.conversations_replies.return_value = _make_replies_result(
            [_make_message("1700000001.000001", "top-level msg")]
        )
        items = list(job.list_items())
        self.assertEqual(len(items), 1)

    def test_yield_messages_skips_replies_api_call_when_no_replies(self):
        job = self._make_job(channel_ids="C123")
        self.mock_client.conversations_history.return_value = _make_history_result(
            [{"ts": "1700000001.000001", "text": "msg no replies", "reply_count": 0}]
        )
        list(job.list_items())
        self.mock_client.conversations_replies.assert_not_called()

    # ------------------------------------------------------------------
    # _fetch_message_with_replies — thread concatenation
    # ------------------------------------------------------------------

    def test_fetch_message_with_replies_concatenates_thread(self):
        self.mock_client.conversations_replies.return_value = (
            _make_replies_result(
                [
                    _make_message("1700000001.000001", "parent msg"),
                    _make_message("1700000001.000002", "reply 1"),
                ]
            )
        )
        job = self._make_job(channel_ids="C123")
        text = job._fetch_message_with_replies("C123", "1700000001.000001")
        self.assertIn("parent msg", text)
        self.assertIn("reply 1", text)

    def test_fetch_message_with_replies_skips_messages_without_text(self):
        self.mock_client.conversations_replies.return_value = (
            _make_replies_result(
                [
                    _make_message("1700000001.000001", "parent msg"),
                    {"ts": "1700000001.000002"},
                    {"ts": "1700000001.000003", "text": ""},
                    _make_message("1700000001.000004", "reply with text"),
                ]
            )
        )
        job = self._make_job(channel_ids="C123")
        text = job._fetch_message_with_replies("C123", "1700000001.000001")
        self.assertIn("parent msg", text)
        self.assertIn("reply with text", text)

    # ------------------------------------------------------------------
    # Integration: process_item
    # ------------------------------------------------------------------

    def test_process_item_calls_vector_store_and_metadata_tracker(self):
        job = self._make_job(channel_ids="C123")
        item = IngestionItem(
            id="slack:test_slack:C123:1700000001.000001",
            source_ref={
                "channel_id": "C123",
                "message_ts": "1700000001.000001",
                "text": "channel content",
            },
        )
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
        job = self._make_job(channel_ids="C123")
        item = IngestionItem(
            id="slack:test_slack:C123:1700000001.000001",
            source_ref={
                "channel_id": "C123",
                "message_ts": "1700000001.000001",
                "text": "same content",
            },
        )
        job._seen_add = Mock(return_value=False)
        job.vector_manager.insert_documents = Mock()

        with patch.object(
            job.metadata_tracker, "get_latest_record", return_value=None
        ):
            with patch.object(job.metadata_tracker, "record_metadata"):
                result = job.process_item(item)

        self.assertEqual(result, 0)
        job.vector_manager.insert_documents.assert_not_called()


if __name__ == "__main__":
    unittest.main()
