import unittest
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import requests

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.outlook_ingestion import OutlookIngestionJob

RECEIVED = "2024-06-01T10:00:00Z"
RECEIVED_DT = datetime(2024, 6, 1, 10, 0, 0, tzinfo=UTC)
AUTH_HEADERS = {"Authorization": "Bearer test-token"}


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


def _token_response(access_token="test-token"):
    """A mocked Graph token endpoint response."""
    response = MagicMock()
    response.json.return_value = {"access_token": access_token} if access_token else {}
    return response


def _make_job(config=None, **cfg_overrides):
    if config is None:
        config = _make_config(**cfg_overrides)
    with (
        patch("tasks.base.MetadataTracker"),
        patch("tasks.base.VectorStoreManager"),
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


class TestOutlookGetAuthHeaders(unittest.TestCase):
    def test_posts_client_credentials_form_to_token_endpoint(self):
        job = _make_job()
        job._session = MagicMock()
        job._session.post.return_value = _token_response("abc123")

        headers = job._get_auth_headers()

        self.assertEqual(headers, {"Authorization": "Bearer abc123"})
        job._session.post.assert_called_once()
        call = job._session.post.call_args
        self.assertEqual(call.args[0], "https://login.microsoftonline.com/tid/oauth2/v2.0/token")
        self.assertEqual(
            call.kwargs["data"],
            {
                "grant_type": "client_credentials",
                "client_id": "cid",
                "client_secret": "csecret",
                "scope": "https://graph.microsoft.com/.default",
            },
        )
        self.assertTrue(call.kwargs.get("retry"))

    def test_raises_when_access_token_missing(self):
        job = _make_job()
        job._session = MagicMock()
        job._session.post.return_value = _token_response(access_token=None)

        with self.assertRaises(RuntimeError):
            job._get_auth_headers()

    def test_propagates_http_error_from_token_endpoint(self):
        job = _make_job()
        job._session = MagicMock()
        response = MagicMock()
        response.raise_for_status.side_effect = requests.HTTPError("token endpoint failed")
        job._session.post.return_value = response

        with self.assertRaises(requests.HTTPError):
            job._get_auth_headers()


class TestOutlookListItems(unittest.TestCase):
    def _job_with_mocked_session(self, **cfg_overrides):
        job = _make_job(**cfg_overrides)
        job._session = MagicMock()
        job._get_auth_headers = MagicMock(return_value=AUTH_HEADERS)
        return job

    def test_yields_correct_items(self):
        emails = [_make_email("id1"), _make_email("id2")]
        job = self._job_with_mocked_session()
        messages_lookup = MagicMock()
        messages_lookup.json.return_value = {"value": emails}
        job._session.get.return_value = messages_lookup

        items = list(job.list_items())

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].id, "outlook:id1")
        self.assertEqual(items[0].source_ref["subject"], "Hello World")
        self.assertEqual(items[0].last_modified, RECEIVED_DT)

    def test_skips_email_without_id(self):
        emails = [{"subject": "No ID", "receivedDateTime": RECEIVED, "body": {"content": ""}}]
        job = self._job_with_mocked_session()
        messages_lookup = MagicMock()
        messages_lookup.json.return_value = {"value": emails}
        job._session.get.return_value = messages_lookup

        items = list(job.list_items())

        self.assertEqual(len(items), 0)

    def test_well_known_folder_is_used_directly_without_resolution(self):
        email = _make_email("id1")
        job = self._job_with_mocked_session(folder="Inbox")
        messages_lookup = MagicMock()
        messages_lookup.json.return_value = {"value": [email]}
        job._session.get.return_value = messages_lookup

        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        # Only the message fetch, no folder-tree walk for a well-known name.
        self.assertEqual(job._session.get.call_count, 1)
        called_url = job._session.get.call_args.args[0]
        self.assertIn("/mailFolders/Inbox/messages", called_url)

    def test_resolves_custom_display_name_folder_upfront(self):
        email = _make_email("id1")
        job = self._job_with_mocked_session(folder="Proba")

        folder_lookup = MagicMock()
        folder_lookup.json.return_value = {
            "value": [{"id": "folder-id-123", "displayName": "Proba", "childFolderCount": 0}]
        }
        messages_lookup = MagicMock()
        messages_lookup.json.return_value = {"value": [email]}
        job._session.get.side_effect = [folder_lookup, messages_lookup]

        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "outlook:id1")
        self.assertEqual(job._session.get.call_count, 2)

    def test_resolves_display_name_folder_across_paginated_folder_listing(self):
        email = _make_email("id1")
        job = self._job_with_mocked_session(folder="Proba")

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
        job._session.get.side_effect = [page1, page2, messages_lookup]

        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "outlook:id1")

    def test_resolves_display_name_folder_in_nested_child_folder(self):
        email = _make_email("id1")
        job = self._job_with_mocked_session(folder="Proba")

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
        job._session.get.side_effect = [top_level, child_level, messages_lookup]

        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "outlook:id1")

    def test_raises_clear_error_when_folder_display_name_cannot_be_resolved(self):
        job = self._job_with_mocked_session(folder="Missing Folder")

        folder_lookup = MagicMock()
        folder_lookup.json.return_value = {"value": []}
        job._session.get.return_value = folder_lookup

        with self.assertRaises(ValueError):
            list(job.list_items())

        # Only the folder-tree lookup happened — no message fetch was attempted.
        job._session.get.assert_called_once()

    def test_resolved_folder_id_is_cached_across_calls(self):
        email = _make_email("id1")
        job = self._job_with_mocked_session(folder="Proba")

        folder_lookup = MagicMock()
        folder_lookup.json.return_value = {
            "value": [{"id": "folder-id-123", "displayName": "Proba", "childFolderCount": 0}]
        }
        messages_lookup = MagicMock()
        messages_lookup.json.return_value = {"value": [email]}
        job._session.get.side_effect = [folder_lookup, messages_lookup, messages_lookup]

        list(job.list_items())
        list(job.list_items())

        # First call: 1 folder lookup + 1 message fetch. Second call: message fetch only.
        self.assertEqual(job._session.get.call_count, 3)


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

    def test_html_body_is_converted_when_html_to_text_true(self):
        email = _make_email(body_content="<p>Hello</p>")
        email["body"]["contentType"] = "html"
        item = IngestionItem(id="outlook:id1", source_ref=email)
        content = _make_job(html_to_text="true").get_raw_content(item)
        self.assertNotIn("<p>", content)
        self.assertIn("Hello", content)

    def test_html_body_is_kept_raw_when_html_to_text_false(self):
        email = _make_email(body_content="<p>Hello</p>")
        email["body"]["contentType"] = "html"
        item = IngestionItem(id="outlook:id1", source_ref=email)
        content = _make_job(html_to_text="false").get_raw_content(item)
        self.assertIn("<p>Hello</p>", content)

    def test_plain_text_body_is_not_converted(self):
        email = _make_email(body_content="Plain text body.")
        email["body"]["contentType"] = "text"
        item = IngestionItem(id="outlook:id1", source_ref=email)
        content = _make_job().get_raw_content(item)
        self.assertIn("Plain text body.", content)


class TestOutlookGetItemName(unittest.TestCase):
    def test_basic(self):
        item = IngestionItem(id="outlook:abc123", source_ref={})
        self.assertEqual(_make_job().get_item_name(item), "outlook_abc123")

    def test_truncated_to_255(self):
        item = IngestionItem(id="outlook:" + "x" * 300, source_ref={})
        self.assertEqual(len(_make_job().get_item_name(item)), 255)


class TestOutlookGetItemChecksum(unittest.TestCase):
    def test_returns_checksum_when_id_and_received_present(self):
        email = _make_email("msg-42")
        item = IngestionItem(id="outlook:msg-42", source_ref=email)
        checksum = _make_job().get_item_checksum(item)
        self.assertEqual(checksum, f"msg-42:{RECEIVED}")

    def test_returns_none_when_id_missing(self):
        email = _make_email()
        del email["id"]
        item = IngestionItem(id="outlook:x", source_ref=email)
        self.assertIsNone(_make_job().get_item_checksum(item))

    def test_returns_none_when_received_missing(self):
        email = _make_email()
        del email["receivedDateTime"]
        item = IngestionItem(id="outlook:x", source_ref=email)
        self.assertIsNone(_make_job().get_item_checksum(item))


class TestOutlookGetExtraMetadata(unittest.TestCase):
    def test_includes_outlook_fields(self):
        email = _make_email(subject="Status Update", sender="bob@example.com")
        item = IngestionItem(id="outlook:id1", source_ref=email, last_modified=RECEIVED_DT)
        meta = _make_job().get_extra_metadata(item, "", {})

        self.assertEqual(meta["user_email"], "user@company.com")
        self.assertEqual(meta["folder"], "Inbox")
        self.assertEqual(meta["subject"], "Status Update")
        self.assertEqual(meta["sender"], "bob@example.com")
        self.assertEqual(meta["received_at"], RECEIVED)

    def test_includes_web_link(self):
        email = _make_email()
        email["webLink"] = "https://outlook.office.com/mail/id/AAMk"
        item = IngestionItem(id="outlook:id1", source_ref=email)
        meta = _make_job().get_extra_metadata(item, "", {})
        self.assertEqual(meta["web_link"], "https://outlook.office.com/mail/id/AAMk")

    def test_web_link_defaults_to_empty_string(self):
        email = _make_email()
        item = IngestionItem(id="outlook:id1", source_ref=email)
        meta = _make_job().get_extra_metadata(item, "", {})
        self.assertEqual(meta["web_link"], "")


if __name__ == "__main__":
    unittest.main()
