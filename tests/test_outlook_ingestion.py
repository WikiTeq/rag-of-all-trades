import unittest
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import requests

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.outlook_ingestion import OutlookIngestionJob

RECEIVED = "2024-06-01T10:00:00Z"
RECEIVED_DT = datetime(2024, 6, 1, 10, 0, 0, tzinfo=UTC)


def _make_config(**overrides):
    cfg = {
        "client_id": "cid",
        "client_secret": "csecret",
        "tenant_id": "tid",
        "user_email": "user@company.com",
    }
    cfg.update(overrides)
    return {"name": "test_outlook", "config": cfg}


def _make_email(
    eid="msg-1",
    subject="Hello World",
    sender="alice@example.com",
    received=RECEIVED,
    body_content="Email body text.",
):
    return {
        "id": eid,
        "subject": subject,
        "from": {"emailAddress": {"address": sender}},
        "receivedDateTime": received,
        "body": {"content": body_content},
    }


def _make_job(config=None, **cfg_overrides):
    if config is None:
        config = _make_config(**cfg_overrides)
    with (
        patch("tasks.base.MetadataTracker"),
        patch("tasks.base.VectorStoreManager"),
        patch("tasks.outlook_ingestion.OutlookEmailReader"),
    ):
        return OutlookIngestionJob(config)


class TestOutlookIngestionInit(unittest.TestCase):
    def test_source_type(self):
        self.assertEqual(_make_job().source_type, "outlook")

    def test_missing_required_fields_raises(self):
        required = ["client_id", "client_secret", "tenant_id", "user_email"]
        for field in required:
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    _make_job(**{field: ""})

    def test_non_positive_num_mails_raises(self):
        with self.assertRaises(ValueError):
            _make_job(num_mails=0)

    def test_defaults(self):
        job = _make_job()
        self.assertEqual(job.folder, "Inbox")
        self.assertEqual(job.num_mails, 10)


class TestOutlookListItems(unittest.TestCase):
    def _mock_reader(self, emails):
        mock = MagicMock()
        mock._fetch_emails.return_value = emails
        mock._authorization_headers = {"Authorization": "Bearer token"}
        return mock

    def test_yields_correct_items(self):
        emails = [_make_email("id1"), _make_email("id2")]
        job = _make_job()
        job._reader = self._mock_reader(emails)

        items = list(job.list_items())

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].id, "outlook:id1")
        self.assertEqual(items[0].source_ref["subject"], "Hello World")
        self.assertEqual(items[0].last_modified, RECEIVED_DT)

    def test_skips_email_without_id(self):
        emails = [{"subject": "No ID", "receivedDateTime": RECEIVED, "body": {"content": ""}}]
        job = _make_job()
        job._reader = self._mock_reader(emails)

        items = list(job.list_items())

        self.assertEqual(len(items), 0)

    def test_resolves_display_name_folder_when_graph_path_returns_400(self):
        email = _make_email("id1")
        response_400 = MagicMock(status_code=400)
        reader = self._mock_reader([])
        reader._fetch_emails.side_effect = requests.HTTPError(response=response_400)

        folder_lookup = MagicMock()
        folder_lookup.json.return_value = {
            "value": [{"id": "folder-id-123", "displayName": "Proba", "childFolderCount": 0}]
        }

        messages_lookup = MagicMock()
        messages_lookup.json.return_value = {"value": [email]}

        job = _make_job(folder="Proba")
        job._reader = reader

        with patch("tasks.outlook_ingestion.requests.get", side_effect=[folder_lookup, messages_lookup]) as mock_get:
            items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "outlook:id1")
        self.assertEqual(mock_get.call_count, 2)

    def test_resolves_display_name_folder_across_paginated_folder_listing(self):
        email = _make_email("id1")
        response_400 = MagicMock(status_code=400)
        reader = self._mock_reader([])
        reader._fetch_emails.side_effect = requests.HTTPError(response=response_400)

        page1 = MagicMock()
        page1.json.return_value = {
            "value": [{"id": "other-id", "displayName": "Other", "childFolderCount": 0}],
            "@odata.nextLink": "https://graph.microsoft.com/v1.0/users/u/mailFolders?$skiptoken=abc",
        }
        page2 = MagicMock()
        page2.json.return_value = {
            "value": [{"id": "folder-id-page2", "displayName": "Proba", "childFolderCount": 0}],
        }
        messages_lookup = MagicMock()
        messages_lookup.json.return_value = {"value": [email]}

        job = _make_job(folder="Proba")
        job._reader = reader

        with patch("tasks.outlook_ingestion.requests.get", side_effect=[page1, page2, messages_lookup]):
            items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "outlook:id1")

    def test_resolves_display_name_folder_in_nested_child_folder(self):
        email = _make_email("id1")
        response_400 = MagicMock(status_code=400)
        reader = self._mock_reader([])
        reader._fetch_emails.side_effect = requests.HTTPError(response=response_400)

        top_level = MagicMock()
        top_level.json.return_value = {
            "value": [{"id": "parent-id", "displayName": "Parent", "childFolderCount": 1}],
        }
        child_level = MagicMock()
        child_level.json.return_value = {
            "value": [{"id": "child-id", "displayName": "Proba", "childFolderCount": 0}],
        }
        messages_lookup = MagicMock()
        messages_lookup.json.return_value = {"value": [email]}

        job = _make_job(folder="Proba")
        job._reader = reader

        with patch("tasks.outlook_ingestion.requests.get", side_effect=[top_level, child_level, messages_lookup]):
            items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "outlook:id1")

    def test_raises_original_error_when_folder_display_name_cannot_be_resolved(self):
        response_400 = MagicMock(status_code=400)
        reader = self._mock_reader([])
        error = requests.HTTPError(response=response_400)
        reader._fetch_emails.side_effect = error

        folder_lookup = MagicMock()
        folder_lookup.json.return_value = {"value": []}

        job = _make_job(folder="Missing Folder")
        job._reader = reader

        with (
            patch("tasks.outlook_ingestion.requests.get", return_value=folder_lookup),
            self.assertRaises(requests.HTTPError),
        ):
            list(job.list_items())


class TestOutlookGetRawContent(unittest.TestCase):
    def test_includes_subject_and_body(self):
        email = _make_email(subject="Re: Meeting", body_content="See you there.")
        item = IngestionItem(id="outlook:id1", source_ref=email)
        content = _make_job().get_raw_content(item)

        self.assertIn("# Re: Meeting", content)
        self.assertIn("See you there.", content)
        self.assertIn("alice@example.com", content)

    def test_no_subject_fallback(self):
        email = _make_email(subject=None)
        item = IngestionItem(id="outlook:id1", source_ref=email)
        self.assertIn("(no subject)", _make_job().get_raw_content(item))

    def test_missing_sender_is_empty(self):
        email = _make_email()
        del email["from"]
        item = IngestionItem(id="outlook:id1", source_ref=email)
        self.assertIn("**From:**", _make_job().get_raw_content(item))


class TestOutlookGetItemName(unittest.TestCase):
    def test_basic(self):
        item = IngestionItem(id="outlook:abc123", source_ref={})
        self.assertEqual(_make_job().get_item_name(item), "outlook_abc123")

    def test_truncated_to_255(self):
        item = IngestionItem(id="outlook:" + "x" * 300, source_ref={})
        self.assertEqual(len(_make_job().get_item_name(item)), 255)


class TestOutlookGetDocumentMetadata(unittest.TestCase):
    def test_includes_outlook_fields(self):
        email = _make_email(subject="Status Update", sender="bob@example.com")
        item = IngestionItem(id="outlook:id1", source_ref=email, last_modified=RECEIVED_DT)
        meta = _make_job().get_document_metadata(item, "outlook_id1", "checksum", 1, RECEIVED_DT)

        self.assertEqual(meta["user_email"], "user@company.com")
        self.assertEqual(meta["folder"], "Inbox")
        self.assertEqual(meta["subject"], "Status Update")
        self.assertEqual(meta["sender"], "bob@example.com")
        self.assertEqual(meta["received_at"], RECEIVED)
        self.assertEqual(meta["source"], "outlook")


if __name__ == "__main__":
    unittest.main()
