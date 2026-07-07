import email
import imaplib
import unittest
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from unittest.mock import MagicMock, patch

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.imap_ingestion import IMAPIngestionJob, _decode_header_value, _extract_body
from utils.text import html_to_markdown


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

    def test_init_log_does_not_leak_username(self):
        with self.assertLogs("tasks.imap_ingestion", level="INFO") as log_ctx:
            _make_job()
        init_messages = "\n".join(log_ctx.output)
        self.assertNotIn("user@example.com", init_messages)

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

    def test_connect_uses_bounded_timeout(self):
        job = _make_job()
        with patch("tasks.imap_ingestion.imaplib.IMAP4_SSL") as mock_imap_ssl:
            job._connect()
        mock_imap_ssl.assert_called_once_with(job.host, job.port, timeout=job._CONNECT_TIMEOUT)


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
        conn = MagicMock(spec=imaplib.IMAP4_SSL)
        conn.select.return_value = ("OK", [b"1"])
        conn.uid.return_value = ("OK", [b"1 2"])
        headers = {
            b"1": email.message_from_bytes(_make_raw_email(message_id="<one@example.com>")),
            b"2": email.message_from_bytes(_make_raw_email(message_id="<two@example.com>")),
        }

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

        # Both mailboxes contain the same Message-ID; list_items() must dedup
        # across mailboxes and yield it only once.
        self.assertEqual(len(items), 1)
        self.assertIn("<same@example.com>", items[0].id)

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
        # The session opened for this call is not reused across a list_items()
        # run, so it must be torn down and cleared even on a failed fetch.
        mock_conn.logout.assert_called_once()
        self.assertIsNone(job._run_conn)
        self.assertIsNone(job._selected_mailbox)

    def test_reuses_connection_from_list_items_run(self):
        """get_raw_content() must not open a new IMAP session per item while a
        run (list_items() generator) is still active — it should reuse the
        same connection instead of reconnecting per message."""
        job = _make_job(mailboxes="INBOX")
        raw1 = _make_raw_email(message_id="<one@example.com>")
        raw2 = _make_raw_email(message_id="<two@example.com>")
        conn = MagicMock(spec=imaplib.IMAP4_SSL)
        conn.select.return_value = ("OK", [b"1"])
        conn.uid.return_value = ("OK", [b"1 2"])
        headers = {
            b"1": email.message_from_bytes(raw1),
            b"2": email.message_from_bytes(raw2),
        }

        with patch.object(job, "_connect", return_value=conn) as mock_connect:
            with patch.object(job, "_fetch_headers_batch", return_value=headers):
                with patch.object(
                    job,
                    "_fetch_full_message",
                    side_effect=[email.message_from_bytes(raw1), email.message_from_bytes(raw2)],
                ):
                    gen = job.list_items()
                    item1 = next(gen)
                    job.get_raw_content(item1)
                    item2 = next(gen)
                    job.get_raw_content(item2)
                    with self.assertRaises(StopIteration):
                        next(gen)

        mock_connect.assert_called_once()
        conn.logout.assert_called_once()

    def test_reuses_connection_across_mailbox_switch(self):
        """get_raw_content() must re-select the mailbox on the shared connection
        when the item it's fetching belongs to a different mailbox than the one
        currently selected — this happens when list_items() has moved on to a
        second mailbox between yields, and get_raw_content() is called for an
        item from a different mailbox than the current selection."""
        job = _make_job(mailboxes="INBOX,Sent")
        raw_inbox = _make_raw_email(subject="Inbox Subject", message_id="<inbox@example.com>")
        raw_sent = _make_raw_email(subject="Sent Subject", message_id="<sent@example.com>")

        conn = MagicMock(spec=imaplib.IMAP4_SSL)
        conn.select.return_value = ("OK", [b"1"])

        def fake_uid(command, *args, **kwargs):
            if command == "search":
                return ("OK", [b"1"])
            return ("OK", [])

        conn.uid.side_effect = fake_uid

        headers_by_mailbox = {
            "INBOX": {b"1": email.message_from_bytes(raw_inbox)},
            "Sent": {b"1": email.message_from_bytes(raw_sent)},
        }

        def fake_fetch_headers_batch(conn, uids):
            return headers_by_mailbox[job._selected_mailbox]

        full_messages_by_mailbox = {
            "INBOX": email.message_from_bytes(raw_inbox),
            "Sent": email.message_from_bytes(raw_sent),
        }

        def fake_fetch_full_message(conn, uid):
            return full_messages_by_mailbox[job._selected_mailbox]

        with patch.object(job, "_connect", return_value=conn) as mock_connect:
            with patch.object(job, "_fetch_headers_batch", side_effect=fake_fetch_headers_batch):
                with patch.object(job, "_fetch_full_message", side_effect=fake_fetch_full_message):
                    gen = job.list_items()
                    item_inbox = next(gen)  # yielded while INBOX is selected
                    item_sent = next(gen)  # list_items() has moved on to Sent by now

                    self.assertEqual(item_inbox.source_ref["mailbox"], "INBOX")
                    self.assertEqual(item_sent.source_ref["mailbox"], "Sent")

                    # Fetch out of yield order: Sent (currently selected) then INBOX
                    # (a different mailbox than what's currently selected on conn).
                    content_sent = job.get_raw_content(item_sent)
                    content_inbox = job.get_raw_content(item_inbox)

                    with self.assertRaises(StopIteration):
                        next(gen)

        self.assertIn("Sent Subject", content_sent)
        self.assertIn("Inbox Subject", content_inbox)
        # Only one IMAP4_SSL session for the whole run, despite fetching from
        # two different mailboxes via get_raw_content().
        mock_connect.assert_called_once()
        conn.logout.assert_called_once()


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


class TestIMAPFetchHeadersBatch(unittest.TestCase):
    def test_batches_uids_in_chunks(self):
        job = _make_job(mailboxes="INBOX")
        raw = _make_raw_email()
        uids = [str(i).encode() for i in range(1, 251)]  # 250 uids -> 3 batches of 100
        chunk_sizes = []

        def fake_uid(*args, **kwargs):
            chunk = args[1].split(b",")
            chunk_sizes.append(len(chunk))
            data = []
            for uid in chunk:
                data.append((f"1 (UID {uid.decode()} RFC822.HEADER {{10}}".encode(), raw))
            return "OK", data

        conn = MagicMock(spec=imaplib.IMAP4_SSL)
        conn.uid.side_effect = fake_uid

        result = job._fetch_headers_batch(conn, uids)

        self.assertEqual(conn.uid.call_count, 3)
        self.assertEqual(len(result), 250)
        self.assertTrue(all(size <= job._FETCH_BATCH_SIZE for size in chunk_sizes))
        self.assertEqual(chunk_sizes, [100, 100, 50])


class TestIMAPHelpers(unittest.TestCase):
    def test_strip_html(self):
        self.assertNotIn("<", html_to_markdown("<p>Hello <b>world</b></p>"))
        self.assertIn("Hello", html_to_markdown("<p>Hello <b>world</b></p>"))

    def test_decode_header_value_plain(self):
        self.assertEqual(_decode_header_value("Hello"), "Hello")

    def test_decode_header_value_encoded(self):
        result = _decode_header_value("=?utf-8?b?SGVsbG8gV29ybGQ=?=")
        self.assertEqual(result, "Hello World")

    def test_decode_header_value_unknown_charset_falls_back_to_utf8(self):
        # =?bogus-charset?B?SGVsbG8=?= decodes to b"Hello" under an unregistered
        # codec name; decode_header() doesn't validate the charset itself, so
        # the LookupError only surfaces on .decode() and must be handled there.
        result = _decode_header_value("=?bogus-charset?B?SGVsbG8=?=")
        self.assertEqual(result, "Hello")

    def test_extract_body_plain(self):
        msg = MIMEText("Plain text body", "plain")
        result = _extract_body(email.message_from_bytes(msg.as_bytes()))
        self.assertEqual(result, "Plain text body")

    def test_extract_body_unknown_charset_falls_back_to_utf8(self):
        raw = b"Content-Type: text/plain; charset=bogus-charset-xyz\n\nHello World"
        result = _extract_body(email.message_from_bytes(raw))
        self.assertEqual(result, "Hello World")

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
