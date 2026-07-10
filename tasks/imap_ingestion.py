import imaplib
import logging
import re
import ssl
from collections.abc import Iterable
from datetime import UTC, datetime
from email import message_from_bytes
from email.header import decode_header
from email.message import Message
from email.utils import parsedate_to_datetime
from typing import Any

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.parse import parse_bool, parse_list
from utils.text import html_to_markdown, slugify

logger = logging.getLogger(__name__)

# imaplib.IMAP4_SSL (implicit TLS) and imaplib.IMAP4 (used with STARTTLS) share
# the same select/uid/logout/list API surface used throughout this connector.
IMAPConnection = imaplib.IMAP4 | imaplib.IMAP4_SSL


def _decode_header_value(raw: str | bytes | None) -> str:
    if not raw:
        return ""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    parts = []
    for fragment, charset in decode_header(raw):
        if isinstance(fragment, bytes):
            try:
                parts.append(fragment.decode(charset or "utf-8", errors="replace"))
            except LookupError:
                parts.append(fragment.decode("utf-8", errors="replace"))
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


def _decode_payload(payload: bytes, charset: str) -> str:
    try:
        return payload.decode(charset, errors="replace")
    except LookupError:
        return payload.decode("utf-8", errors="replace")


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
            text = _decode_payload(payload, charset)
            if ct == "text/plain":
                plain.append(text)
            elif ct == "text/html":
                html.append(text)
    else:
        ct = msg.get_content_type()
        charset = msg.get_content_charset() or "utf-8"
        payload = msg.get_payload(decode=True)
        if payload:
            text = _decode_payload(payload, charset)
            if ct == "text/html":
                html.append(text)
            else:
                plain.append(text)

    if plain:
        return "\n".join(plain).strip()
    if html:
        return html_to_markdown("\n".join(html))
    return ""


class IMAPIngestionJob(IngestionJob):
    """Ingestion connector for IMAP email mailboxes.

    Connects to an IMAP server via implicit TLS (IMAP4_SSL) or STARTTLS and ingests emails as documents.
    Each email becomes one document with subject as title and body as content.
    Mailboxes are auto-discovered when not specified in config.

    Configuration (config.yaml):
        - config.host: IMAP server hostname (required)
        - config.port: IMAP server port (optional, default 993, or 143 when config.use_starttls is set)
        - config.username: IMAP account username (required)
        - config.password: IMAP account password or app-specific password (required)
        - config.mailboxes: comma-separated list of mailboxes to ingest (optional, default: all)
        - config.since: only ingest messages on or after this date, e.g. "2024-01-01" (optional)
        - config.use_starttls: connect over plaintext then upgrade via STARTTLS instead of
          implicit TLS (optional, default False)
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

        self.use_starttls = parse_bool(cfg.get("use_starttls"))
        self.port = int(cfg.get("port", 143 if self.use_starttls else 993))

        self.username = cfg.get("username", "").strip()
        if not self.username:
            raise ValueError("username is required in IMAP connector config")

        self.password = cfg.get("password", "").strip()
        if not self.password:
            raise ValueError("password is required in IMAP connector config")

        self.mailboxes = parse_list(cfg.get("mailboxes", ""))

        since_str = cfg.get("since", "").strip()
        self.since: datetime | None = self._parse_since(since_str) if since_str else None

        # Shared connection + currently selected mailbox, reused across list_items()
        # and get_raw_content() for the duration of a single run().
        self._run_conn: IMAPConnection | None = None
        self._selected_mailbox: str | None = None

        logger.info(f"Initialized IMAP connector for {self.host}:{self.port}")

    @staticmethod
    def _parse_since(value: str) -> datetime:
        """Parse a date string (YYYY-MM-DD) into a datetime object."""
        try:
            return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
        except ValueError:
            raise ValueError(f"Invalid date format {value!r} in IMAP connector config. Expected YYYY-MM-DD.")

    _CONNECT_TIMEOUT = 30

    def _connect(self) -> IMAPConnection:
        ssl_context = ssl.create_default_context()
        if self.use_starttls:
            conn = imaplib.IMAP4(self.host, self.port, timeout=self._CONNECT_TIMEOUT)
            conn.starttls(ssl_context)
            if not conn._tls_established:
                # starttls() raises on failure, so this should be unreachable, but
                # never send credentials over a connection that isn't verified as TLS.
                raise RuntimeError("STARTTLS did not establish a TLS session")
        else:
            conn = imaplib.IMAP4_SSL(self.host, self.port, ssl_context=ssl_context, timeout=self._CONNECT_TIMEOUT)
        conn.login(self.username, self.password)
        return conn

    def _get_run_conn(self) -> IMAPConnection:
        """Return the connection shared for this run, opening one if needed."""
        if self._run_conn is None:
            self._run_conn = self._connect()
            self._selected_mailbox = None
        return self._run_conn

    def _ensure_mailbox_selected(self, mailbox: str) -> bool:
        conn = self._get_run_conn()
        if self._selected_mailbox == mailbox:
            return True
        if not self._select_mailbox(conn, mailbox):
            return False
        self._selected_mailbox = mailbox
        return True

    def _discover_mailboxes(self, conn: IMAPConnection) -> list[str]:
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

    def _select_mailbox(self, conn: IMAPConnection, mailbox: str) -> bool:
        typ, _ = conn.select(f'"{mailbox}"', readonly=True)
        if typ != "OK":
            logger.warning(f"[{self.source_name}] Cannot select mailbox {mailbox!r}, skipping")
            return False
        return True

    def _fetch_all_uids(self, conn: IMAPConnection) -> list[bytes]:
        if self.since:
            # RFC 3501 SEARCH date format is DD-Mon-YYYY (e.g. "01-Jan-2025").
            typ, data = conn.uid("search", None, "SINCE", self.since.strftime("%d-%b-%Y"))
        else:
            typ, data = conn.uid("search", None, "ALL")
        if typ != "OK" or not data or not data[0]:
            return []
        return data[0].split()

    _FETCH_BATCH_SIZE = 100

    def _fetch_headers_batch(self, conn: IMAPConnection, uids: list[bytes]) -> dict[bytes, Message]:
        """Fetch headers-only for all UIDs, chunked to avoid oversized IMAP commands."""
        result: dict[bytes, Message] = {}
        for i in range(0, len(uids), self._FETCH_BATCH_SIZE):
            chunk = uids[i : i + self._FETCH_BATCH_SIZE]
            uid_set = b",".join(chunk)
            typ, data = conn.uid("fetch", uid_set, "(RFC822.HEADER)")
            if typ != "OK" or not data:
                continue

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

    def _fetch_full_message(self, conn: IMAPConnection, uid: bytes) -> Message | None:
        """Fetch a single full RFC822 message by UID."""
        typ, data = conn.uid("fetch", uid, "(RFC822)")
        if typ != "OK" or not data:
            return None
        for part in data:
            if isinstance(part, tuple) and len(part) >= 2:
                return message_from_bytes(part[1])
        return None

    def list_items(self) -> Iterable[IngestionItem]:
        conn = self._get_run_conn()
        try:
            mailboxes = self.mailboxes if self.mailboxes else self._discover_mailboxes(conn)
            logger.info(f"[{self.source_name}] Ingesting mailboxes: {mailboxes}")

            seen_message_ids: set[str] = set()

            for mailbox in mailboxes:
                if not self._ensure_mailbox_selected(mailbox):
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
                        if message_id in seen_message_ids:
                            continue
                        seen_message_ids.add(message_id)
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
                logger.debug(f"[{self.source_name}] IMAP logout failed", exc_info=True)
            self._run_conn = None
            self._selected_mailbox = None

    def get_item_checksum(self, item: IngestionItem) -> str | None:
        return item._metadata_cache.get("_checksum_key")

    def get_raw_content(self, item: IngestionItem) -> str:
        mailbox = item.source_ref["mailbox"]
        uid = item.source_ref["uid"]

        msg: Message | None = item._metadata_cache.get("_msg")
        if msg is None:
            # Reuse the connection shared with list_items() for this run when available
            # (avoids reconnecting per message); fall back to a private one otherwise.
            reused = self._run_conn is not None
            conn = self._get_run_conn()
            try:
                if not self._ensure_mailbox_selected(mailbox):
                    return ""
                msg = self._fetch_full_message(conn, uid)
            finally:
                if not reused:
                    try:
                        conn.logout()
                    except Exception:
                        logger.debug(f"[{self.source_name}] IMAP logout failed", exc_info=True)
                    self._run_conn = None
                    self._selected_mailbox = None

        if msg is None:
            logger.warning(f"[{self.source_name}] Could not fetch message for item {item.id!r}, skipping")
            return ""

        subject = _decode_header_value(msg.get("Subject"))
        from_ = _decode_header_value(msg.get("From"))
        to_ = _decode_header_value(msg.get("To"))
        cc_ = _decode_header_value(msg.get("Cc"))
        bcc_ = _decode_header_value(msg.get("Bcc"))
        date_str = msg.get("Date") or ""
        body = _extract_body(msg)

        item._metadata_cache.update(
            {
                "subject": subject,
                "from": from_,
                "to": to_,
                "cc": cc_,
                "bcc": bcc_,
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
        if cc_:
            parts.append(f"**Cc:** {cc_}")
        if bcc_:
            parts.append(f"**Bcc:** {bcc_}")
        if date_str:
            parts.append(f"**Date:** {date_str}")
        if body:
            parts.append(body)

        return "\n\n".join(parts)

    def get_item_name(self, item: IngestionItem) -> str:
        return slugify(item.id)

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        cache = item._metadata_cache
        return {
            "subject": cache.get("subject", ""),
            "from": cache.get("from", ""),
            "to": cache.get("to", ""),
            "cc": cache.get("cc", ""),
            "bcc": cache.get("bcc", ""),
            "date": cache.get("date", ""),
            "mailbox": cache.get("mailbox", item.source_ref.get("mailbox", "")),
            "message_id": cache.get("message_id", ""),
        }
