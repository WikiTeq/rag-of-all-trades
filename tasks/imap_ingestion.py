import imaplib
import logging
import re
from collections.abc import Iterable
from datetime import UTC, datetime
from email import message_from_bytes
from email.header import decode_header
from email.message import Message
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import Any

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)


class _HTMLTextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_text(self) -> str:
        return " ".join(self._parts)


def _strip_html(html: str) -> str:
    extractor = _HTMLTextExtractor()
    extractor.feed(html)
    return extractor.get_text()


def _decode_header_value(raw: str | bytes | None) -> str:
    if not raw:
        return ""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    parts = []
    for fragment, charset in decode_header(raw):
        if isinstance(fragment, bytes):
            parts.append(fragment.decode(charset or "utf-8", errors="replace"))
        else:
            parts.append(fragment)
    return "".join(parts)


def _parse_date(date_str: str) -> datetime | None:
    if not date_str:
        return None
    try:
        parsed = parsedate_to_datetime(date_str)
        return parsed.astimezone(UTC)
    except Exception:
        return None


def _extract_body(msg: Message) -> str:
    plain: list[str] = []
    html: list[str] = []

    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if part.get_content_disposition() == "attachment":
                continue
            charset = part.get_content_charset() or "utf-8"
            payload = part.get_payload(decode=True)
            if payload is None:
                continue
            text = payload.decode(charset, errors="replace")
            if ct == "text/plain":
                plain.append(text)
            elif ct == "text/html":
                html.append(text)
    else:
        ct = msg.get_content_type()
        charset = msg.get_content_charset() or "utf-8"
        payload = msg.get_payload(decode=True)
        if payload:
            text = payload.decode(charset, errors="replace")
            if ct == "text/html":
                html.append(text)
            else:
                plain.append(text)

    if plain:
        return "\n".join(plain).strip()
    if html:
        return _strip_html("\n".join(html)).strip()
    return ""


class IMAPIngestionJob(IngestionJob):
    """Ingestion connector for IMAP email mailboxes.

    Connects to an IMAP server via IMAP4_SSL and ingests emails as documents.
    Each email becomes one document with subject as title and body as content.
    Mailboxes are auto-discovered when not specified in config.

    Configuration (config.yaml):
        - config.host: IMAP server hostname (required)
        - config.port: IMAP server port (optional, default 993)
        - config.username: IMAP account username (required)
        - config.password: IMAP account password or app-specific password (required)
        - config.mailboxes: comma-separated list of mailboxes to ingest (optional, default: all)
    """

    @property
    def source_type(self) -> str:
        return "imap"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        self.host = cfg.get("host", "").strip()
        if not self.host:
            raise ValueError("host is required in IMAP connector config")

        self.port = int(cfg.get("port", 993))

        self.username = cfg.get("username", "").strip()
        if not self.username:
            raise ValueError("username is required in IMAP connector config")

        self.password = cfg.get("password", "").strip()
        if not self.password:
            raise ValueError("password is required in IMAP connector config")

        raw_mailboxes = cfg.get("mailboxes", "")
        if isinstance(raw_mailboxes, list):
            self.mailboxes = [m.strip() for m in raw_mailboxes if m.strip()]
        elif raw_mailboxes:
            self.mailboxes = [m.strip() for m in raw_mailboxes.split(",") if m.strip()]
        else:
            self.mailboxes = []

        logger.info(f"Initialized IMAP connector for {self.host}:{self.port} user={self.username}")

    def _connect(self) -> imaplib.IMAP4_SSL:
        conn = imaplib.IMAP4_SSL(self.host, self.port)
        conn.login(self.username, self.password)
        return conn

    def _discover_mailboxes(self, conn: imaplib.IMAP4_SSL) -> list[str]:
        typ, data = conn.list()
        if typ != "OK":
            logger.warning(f"[{self.source_name}] Failed to list mailboxes")
            return []

        mailboxes = []
        for entry in data:
            if not entry:
                continue
            if isinstance(entry, bytes):
                entry = entry.decode("utf-8", errors="replace")
            # LIST response format: (\Flags) "delimiter" "mailbox name"
            match = re.search(r'"([^"]+)"\s*$|(\S+)\s*$', entry)
            if match:
                name = (match.group(1) or match.group(2) or "").strip('"')
                if name:
                    mailboxes.append(name)
        return mailboxes

    def _select_mailbox(self, conn: imaplib.IMAP4_SSL, mailbox: str) -> bool:
        typ, _ = conn.select(f'"{mailbox}"', readonly=True)
        if typ != "OK":
            logger.warning(f"[{self.source_name}] Cannot select mailbox {mailbox!r}, skipping")
            return False
        return True

    def _fetch_all_uids(self, conn: imaplib.IMAP4_SSL) -> list[bytes]:
        typ, data = conn.uid("search", None, "ALL")
        if typ != "OK" or not data or not data[0]:
            return []
        return data[0].split()

    def _fetch_headers_batch(self, conn: imaplib.IMAP4_SSL, uids: list[bytes]) -> dict[bytes, Message]:
        """Fetch headers-only for all UIDs in one IMAP roundtrip."""
        if not uids:
            return {}
        uid_set = b",".join(uids)
        typ, data = conn.uid("fetch", uid_set, "(RFC822.HEADER)")
        if typ != "OK" or not data:
            return {}

        result: dict[bytes, Message] = {}
        for part in data:
            if not isinstance(part, tuple) or len(part) < 2:
                continue
            meta = part[0].decode("utf-8", errors="replace") if isinstance(part[0], bytes) else part[0]
            uid_match = re.search(r"UID (\d+)", meta)
            if not uid_match:
                continue
            uid = uid_match.group(1).encode()
            result[uid] = message_from_bytes(part[1])
        return result

    def _fetch_full_message(self, conn: imaplib.IMAP4_SSL, uid: bytes) -> Message | None:
        """Fetch a single full RFC822 message by UID."""
        typ, data = conn.uid("fetch", uid, "(RFC822)")
        if typ != "OK" or not data:
            return None
        for part in data:
            if isinstance(part, tuple) and len(part) >= 2:
                return message_from_bytes(part[1])
        return None

    def list_items(self) -> Iterable[IngestionItem]:
        conn = self._connect()
        try:
            mailboxes = self.mailboxes if self.mailboxes else self._discover_mailboxes(conn)
            logger.info(f"[{self.source_name}] Ingesting mailboxes: {mailboxes}")

            for mailbox in mailboxes:
                if not self._select_mailbox(conn, mailbox):
                    continue

                uids = self._fetch_all_uids(conn)
                logger.info(f"[{self.source_name}] Mailbox {mailbox!r}: {len(uids)} message(s)")

                if not uids:
                    continue

                headers = self._fetch_headers_batch(conn, uids)

                for uid in uids:
                    hdr = headers.get(uid)
                    if hdr is None:
                        continue

                    message_id = (hdr.get("Message-ID") or "").strip()
                    date_str = hdr.get("Date") or ""
                    last_modified = _parse_date(date_str)

                    if message_id:
                        item_id = f"imap:{self.source_name}:{message_id}"
                    else:
                        item_id = f"imap:{self.source_name}:{mailbox}:{uid.decode()}"

                    item = IngestionItem(
                        id=item_id,
                        source_ref={"mailbox": mailbox, "uid": uid},
                        last_modified=last_modified,
                    )
                    # Cache header-derived fields; full message fetched lazily in get_raw_content
                    item._metadata_cache["_checksum_key"] = f"{message_id or item_id}:{date_str}"
                    yield item
        finally:
            try:
                conn.logout()
            except Exception:
                pass

    def get_item_checksum(self, item: IngestionItem) -> str | None:
        return item._metadata_cache.get("_checksum_key")

    def get_raw_content(self, item: IngestionItem) -> str:
        mailbox = item.source_ref["mailbox"]
        uid = item.source_ref["uid"]

        msg: Message | None = item._metadata_cache.get("_msg")
        if msg is None:
            conn = self._connect()
            try:
                if not self._select_mailbox(conn, mailbox):
                    return ""
                msg = self._fetch_full_message(conn, uid)
            finally:
                try:
                    conn.logout()
                except Exception:
                    pass

        if msg is None:
            logger.warning(f"[{self.source_name}] Could not fetch message for item {item.id!r}, skipping")
            return ""

        subject = _decode_header_value(msg.get("Subject"))
        from_ = _decode_header_value(msg.get("From"))
        to_ = _decode_header_value(msg.get("To"))
        date_str = msg.get("Date") or ""
        body = _extract_body(msg)

        item._metadata_cache.update(
            {
                "subject": subject,
                "from": from_,
                "to": to_,
                "date": date_str,
                "mailbox": mailbox,
                "message_id": (msg.get("Message-ID") or "").strip(),
            }
        )

        parts = [f"# {subject}" if subject else "# (no subject)"]
        if from_:
            parts.append(f"**From:** {from_}")
        if to_:
            parts.append(f"**To:** {to_}")
        if date_str:
            parts.append(f"**Date:** {date_str}")
        if body:
            parts.append(body)

        return "\n\n".join(parts)

    def get_item_name(self, item: IngestionItem) -> str:
        safe = re.sub(r"[^\w\-]", "_", item.id)
        return safe[:255]

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        cache = item._metadata_cache
        return {
            "subject": cache.get("subject", ""),
            "from": cache.get("from", ""),
            "to": cache.get("to", ""),
            "date": cache.get("date", ""),
            "mailbox": cache.get("mailbox", item.source_ref.get("mailbox", "")),
            "message_id": cache.get("message_id", ""),
        }
