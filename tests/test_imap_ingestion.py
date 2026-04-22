import email
import imaplib
import unittest
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from unittest.mock import MagicMock, patch

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.imap_ingestion import IMAPIngestionJob, _decode_header_value, _extract_body, _strip_html


def _make_job(mailboxes=""):
    config = {
        "name": "test-imap",
        "config": {
            "host": "imap.example.com",
            "port": 993,
            "username": "user@example.com",
            "password": "secret",
            "mailboxes": mailboxes,
        },
    }
    with patch("tasks.imap_ingestion.IMAPIngestionJob._connect"):
        job = IMAPIngestionJob(config)
    return job


def _make_raw_email(
    subject="Test Subject",
    from_="sender@example.com",
    to_="recipient@example.com",
    date_="Mon, 1 Jan 2024 10:00:00 +0000",
    message_id="<abc123@mail.example.com>",
    body="Hello world",
    content_type="plain",
) -> bytes:
    msg = MIMEText(body, content_type)
    msg["Subject"] = subject
    msg["From"] = from_
    msg["To"] = to_
    msg["Date"] = date_
    msg["Message-ID"] = message_id
    return msg.as_bytes()


class TestIMAPIngestionInit(unittest.TestCase):
    def test_valid_config(self):
        job = _make_job()
        self.assertEqual(job.host, "imap.example.com")
        self.assertEqual(job.port, 993)
        self.assertEqual(job.username, "user@example.com")
        self.assertEqual(job.mailboxes, [])

    def test_mailboxes_parsed_from_string(self):
        job = _make_job(mailboxes="INBOX,Sent")
        self.assertEqual(job.mailboxes, ["INBOX", "Sent"])

    def test_mailboxes_parsed_from_list(self):
        config = {
            "name": "test",
            "config": {
                "host": "imap.example.com",
                "username": "u",
                "password": "p",
                "mailboxes": ["INBOX", "Sent"],
            },
        }
        with patch("tasks.imap_ingestion.IMAPIngestionJob._connect"):
            job = IMAPIngestionJob(config)
        self.assertEqual(job.mailboxes, ["INBOX", "Sent"])

    def test_missing_host_raises(self):
        with self.assertRaises(ValueError):
            IMAPIngestionJob({"name": "x", "config": {"username": "u", "password": "p"}})

    def test_missing_username_raises(self):
        with self.assertRaises(ValueError):
            IMAPIngestionJob({"name": "x", "config": {"host": "h", "password": "p"}})

    def test_missing_password_raises(self):
        with self.assertRaises(ValueError):
            IMAPIngestionJob({"name": "x", "config": {"host": "h", "username": "u"}})


class TestIMAPListItems(unittest.TestCase):
    def _make_conn_and_headers(self, uids, raw_email, mailbox_list=None):
        conn = MagicMock(spec=imaplib.IMAP4_SSL)
        conn.list.return_value = ("OK", mailbox_list or [b'(\\HasNoChildren) "/" "INBOX"'])
        conn.select.return_value = ("OK", [b"1"])
        conn.uid.return_value = ("OK", [b" ".join(uids)] if uids else [b""])

        headers = {uid: email.message_from_bytes(raw_email) for uid in uids}
        return conn, headers

    def test_list_items_with_configured_mailboxes(self):
        job = _make_job(mailboxes="INBOX")
        raw = _make_raw_email()
        conn, headers = self._make_conn_and_headers([b"1", b"2"], raw)

        with patch.object(job, "_connect", return_value=conn):
            with patch.object(job, "_fetch_headers_batch", return_value=headers):
                items = list(job.list_items())

        self.assertEqual(len(items), 2)
        self.assertTrue(items[0].id.startswith("imap:test-imap:"))

    def test_list_items_dedup_by_message_id(self):
        job = _make_job(mailboxes="INBOX,Sent")
        raw = _make_raw_email(message_id="<same@example.com>")
        conn, headers = self._make_conn_and_headers([b"1"], raw)

        with patch.object(job, "_connect", return_value=conn):
            with patch.object(job, "_fetch_headers_batch", return_value=headers):
                items = list(job.list_items())

        ids = [i.id for i in items]
        # Both mailboxes produce same Message-ID → same item id
        self.assertEqual(ids[0], ids[1])
        self.assertIn("<same@example.com>", ids[0])

    def test_list_items_fallback_id_when_no_message_id(self):
        job = _make_job(mailboxes="INBOX")
        raw = _make_raw_email(message_id="")
        conn, headers = self._make_conn_and_headers([b"42"], raw)

        with patch.object(job, "_connect", return_value=conn):
            with patch.object(job, "_fetch_headers_batch", return_value=headers):
                items = list(job.list_items())

        self.assertIn("INBOX", items[0].id)
        self.assertIn("42", items[0].id)

    def test_list_items_auto_discovers_mailboxes(self):
        job = _make_job()
        raw = _make_raw_email()
        conn, headers = self._make_conn_and_headers([b"1"], raw)

        with patch.object(job, "_connect", return_value=conn):
            with patch.object(job, "_fetch_headers_batch", return_value=headers):
                items = list(job.list_items())

        conn.list.assert_called_once()
        self.assertEqual(len(items), 1)


class TestIMAPGetRawContent(unittest.TestCase):
    def _make_item_with_msg(self, raw: bytes, mailbox: str = "INBOX") -> IngestionItem:
        item = IngestionItem(id="imap:test:123", source_ref={"mailbox": mailbox, "uid": b"1"})
        item._metadata_cache["_msg"] = email.message_from_bytes(raw)
        return item

    def test_plain_text_email(self):
        job = _make_job(mailboxes="INBOX")
        raw = _make_raw_email(subject="Hello", body="Plain body text")
        item = self._make_item_with_msg(raw)
        content = job.get_raw_content(item)
        self.assertIn("# Hello", content)
        self.assertIn("Plain body text", content)
        self.assertIn("sender@example.com", content)

    def test_html_email_stripped(self):
        job = _make_job(mailboxes="INBOX")
        raw = _make_raw_email(body="<b>Bold</b> text", content_type="html")
        item = self._make_item_with_msg(raw)
        content = job.get_raw_content(item)
        self.assertNotIn("<b>", content)
        self.assertIn("Bold", content)
        self.assertIn("text", content)

    def test_metadata_cached(self):
        job = _make_job(mailboxes="INBOX")
        raw = _make_raw_email(subject="Cached Subject", message_id="<cache@test.com>")
        item = self._make_item_with_msg(raw)
        job.get_raw_content(item)
        self.assertEqual(item._metadata_cache["subject"], "Cached Subject")
        self.assertEqual(item._metadata_cache["message_id"], "<cache@test.com>")
        self.assertEqual(item._metadata_cache["mailbox"], "INBOX")

    def test_fetch_fails_returns_empty(self):
        job = _make_job(mailboxes="INBOX")
        item = IngestionItem(id="imap:test:123", source_ref={"mailbox": "INBOX", "uid": b"1"})
        with patch.object(job, "_connect") as mock_connect:
            mock_conn = MagicMock(spec=imaplib.IMAP4_SSL)
            mock_conn.select.return_value = ("OK", [b"1"])
            mock_connect.return_value = mock_conn
            with patch.object(job, "_fetch_full_message", return_value=None):
                content = job.get_raw_content(item)
        self.assertEqual(content, "")


class TestIMAPGetItemName(unittest.TestCase):
    def test_safe_name_from_id(self):
        job = _make_job()
        item = IngestionItem(id="imap:test:<abc@example.com>", source_ref={})
        name = job.get_item_name(item)
        self.assertNotIn("<", name)
        self.assertNotIn(">", name)
        self.assertNotIn("@", name)
        self.assertLessEqual(len(name), 255)

    def test_name_truncated_to_255(self):
        job = _make_job()
        long_id = "imap:test:" + "x" * 300
        item = IngestionItem(id=long_id, source_ref={})
        self.assertLessEqual(len(job.get_item_name(item)), 255)


class TestIMAPGetExtraMetadata(unittest.TestCase):
    def test_returns_expected_keys(self):
        job = _make_job()
        item = IngestionItem(id="imap:test:1", source_ref={"mailbox": "INBOX", "uid": b"1"})
        item._metadata_cache.update(
            {
                "subject": "Test",
                "from": "a@b.com",
                "to": "c@d.com",
                "date": "Mon, 1 Jan 2024",
                "mailbox": "INBOX",
                "message_id": "<x@y>",
            }
        )
        meta = job.get_extra_metadata(item, "", {})
        self.assertEqual(meta["subject"], "Test")
        self.assertEqual(meta["from"], "a@b.com")
        self.assertEqual(meta["mailbox"], "INBOX")
        self.assertEqual(meta["message_id"], "<x@y>")

    def test_does_not_overwrite_reserved_keys(self):
        job = _make_job()
        item = IngestionItem(id="imap:test:1", source_ref={"mailbox": "INBOX", "uid": b"1"})
        meta = job.get_extra_metadata(item, "", {})
        reserved = {"source", "key", "checksum", "version", "format", "source_name", "file_name", "last_modified"}
        self.assertTrue(reserved.isdisjoint(meta.keys()))


class TestIMAPHelpers(unittest.TestCase):
    def test_strip_html(self):
        self.assertNotIn("<", _strip_html("<p>Hello <b>world</b></p>"))
        self.assertIn("Hello", _strip_html("<p>Hello <b>world</b></p>"))

    def test_decode_header_value_plain(self):
        self.assertEqual(_decode_header_value("Hello"), "Hello")

    def test_decode_header_value_encoded(self):
        result = _decode_header_value("=?utf-8?b?SGVsbG8gV29ybGQ=?=")
        self.assertEqual(result, "Hello World")

    def test_extract_body_plain(self):
        msg = MIMEText("Plain text body", "plain")
        result = _extract_body(email.message_from_bytes(msg.as_bytes()))
        self.assertEqual(result, "Plain text body")

    def test_extract_body_html_stripped(self):
        msg = MIMEText("<p>HTML body</p>", "html")
        result = _extract_body(email.message_from_bytes(msg.as_bytes()))
        self.assertIn("HTML body", result)
        self.assertNotIn("<p>", result)

    def test_extract_body_multipart_prefers_plain(self):
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText("Plain part", "plain"))
        msg.attach(MIMEText("<p>HTML part</p>", "html"))
        result = _extract_body(email.message_from_bytes(msg.as_bytes()))
        self.assertEqual(result, "Plain part")


if __name__ == "__main__":
    unittest.main()
