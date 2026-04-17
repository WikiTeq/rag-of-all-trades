import logging
from collections import deque
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

import requests
from llama_index.readers.microsoft_outlook_emails import OutlookEmailReader

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)

GRAPH_API_BASE = "https://graph.microsoft.com/v1.0"
REQUEST_TIMEOUT = (5, 30)  # (connect timeout, read timeout) in seconds


class OutlookIngestionJob(IngestionJob):
    """Ingestion connector for Microsoft Outlook email via Microsoft Graph API.

    Uses the LlamaIndex OutlookEmailReader for authentication and email fetching.
    Requires an Azure app registration with Mail.Read application permission and
    admin consent. Only supported for Microsoft 365 / OneDrive for Business accounts
    (client credentials flow is not available for personal Microsoft accounts).

    Configuration (config.yaml):
        - config.client_id: Azure app registration client ID (required)
        - config.client_secret: Azure app registration client secret (required)
        - config.tenant_id: Azure tenant / directory ID (required)
        - config.user_email: Mailbox owner email address (required)
        - config.folder: Mail folder name (optional, default "Inbox")
        - config.num_mails: Maximum number of emails to fetch (optional, default 10)
    """

    @property
    def source_type(self) -> str:
        return "outlook"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        self.client_id = cfg.get("client_id", "").strip()
        if not self.client_id:
            raise ValueError("client_id is required in Outlook connector config")

        self.client_secret = cfg.get("client_secret", "").strip()
        if not self.client_secret:
            raise ValueError("client_secret is required in Outlook connector config")

        self.tenant_id = cfg.get("tenant_id", "").strip()
        if not self.tenant_id:
            raise ValueError("tenant_id is required in Outlook connector config")

        self.user_email = cfg.get("user_email", "").strip()
        if not self.user_email:
            raise ValueError("user_email is required in Outlook connector config")

        self.folder = str(cfg.get("folder", "Inbox")).strip() or "Inbox"
        self.num_mails = int(cfg.get("num_mails", 10))
        if self.num_mails <= 0:
            raise ValueError("num_mails must be positive in Outlook connector config")

        self._resolved_folder_id: str | None = None

        self._reader = OutlookEmailReader(
            client_id=self.client_id,
            client_secret=self.client_secret,
            tenant_id=self.tenant_id,
            user_email=self.user_email,
            folder=self.folder,
            num_mails=self.num_mails,
        )

        logger.info(
            f"Initialized Outlook connector [{self.source_name}] (folder={self.folder!r}, num_mails={self.num_mails})"
        )

    def list_items(self) -> Iterator[IngestionItem]:
        """Fetch emails via OutlookEmailReader and yield one IngestionItem per email.

        Uses the reader's internal ``_fetch_emails()`` to obtain raw Graph API
        email dicts (including stable message IDs and timestamps) since
        ``load_data()`` returns only plain-text strings with no IDs.
        """
        logger.info(f"[{self.source_name}] Listing emails in folder {self.folder!r}")

        self._reader._ensure_token()
        emails = self._fetch_emails(self._reader)

        yielded = 0
        for email in emails:
            email_id = email.get("id")
            if not email_id:
                logger.warning(f"[{self.source_name}] Skipping email with no id")
                continue

            last_modified = self._parse_datetime(email.get("receivedDateTime"))
            yield IngestionItem(
                id=f"outlook:{email_id}",
                source_ref=email,
                last_modified=last_modified,
            )
            yielded += 1

        logger.info(f"[{self.source_name}] Found {yielded} email(s)")

    def _fetch_emails(self, reader: OutlookEmailReader) -> list[dict[str, Any]]:
        """Fetch emails using the LlamaIndex reader.

        The LlamaIndex OutlookEmailReader passes the folder value directly into
        the Graph API URL (/mailFolders/{folder}/messages). The Graph API only
        accepts well-known folder names (e.g. Inbox, SentItems) or folder IDs
        at that path — custom display names return 400.

        When a 400 is returned, fall back to resolving the folder display name
        to a folder ID via _resolve_folder_id() and retry with the ID.
        """
        headers = self._get_reader_headers(reader)

        if self._resolved_folder_id:
            return self._fetch_emails_from_folder_id(headers, self._resolved_folder_id)

        try:
            return reader._fetch_emails()
        except requests.HTTPError as exc:
            if exc.response is None or exc.response.status_code != 400:
                raise

            folder_id = self._resolve_folder_id(headers)
            if not folder_id:
                logger.error(
                    "[%s] Graph returned 400 for folder %r and display-name lookup found no match",
                    self.source_name,
                    self.folder,
                )
                raise

            logger.info(
                "[%s] Resolved Outlook folder display name %r to Graph folder id %r",
                self.source_name,
                self.folder,
                folder_id,
            )
            self._resolved_folder_id = folder_id
            return self._fetch_emails_from_folder_id(headers, folder_id)

    def _get_reader_headers(self, reader: OutlookEmailReader) -> dict[str, str]:
        """Extract the Bearer token headers from the reader after token initialization."""
        headers = getattr(reader, "_authorization_headers", None)
        if not headers:
            raise RuntimeError("Outlook reader did not expose authorization headers after token initialization")
        return headers

    def _resolve_folder_id(self, headers: dict[str, str]) -> str | None:
        """Resolve a folder display name to a Graph API folder ID.

        The Graph API /mailFolders/{id} endpoint does not accept custom folder
        display names — only well-known names or folder IDs. This method walks
        the full folder tree (BFS, including subfolders and paginated results)
        to find a folder whose displayName matches self.folder (case-insensitive)
        and returns its ID.

        Returns None if no matching folder is found.
        """
        target_name = self.folder.strip().casefold()
        if not target_name:
            return None

        # BFS over the folder tree; queue starts with the top-level mailFolders endpoint
        queue: deque[str] = deque([f"{self._user_mail_folders_url()}/mailFolders"])
        while queue:
            base_url = queue.popleft()
            url: str | None = base_url
            # follow @odata.nextLink pagination within each level;
            # params are only sent on the initial request — nextLink URLs are opaque
            # and already contain all query parameters
            is_first = True
            while url:
                response = requests.get(
                    url,
                    headers=headers,
                    params={"$top": 100, "includeHiddenFolders": "true"} if is_first else None,
                    timeout=REQUEST_TIMEOUT,
                )
                is_first = False
                response.raise_for_status()
                payload = response.json()

                for folder in payload.get("value", []):
                    display_name = (folder.get("displayName") or "").strip()
                    if display_name.casefold() == target_name:
                        return folder.get("id")

                    # enqueue child folders for BFS
                    if folder.get("childFolderCount", 0) > 0 and folder.get("id"):
                        queue.append(
                            f"{self._user_mail_folders_url()}/mailFolders/{quote(folder['id'], safe='')}/childFolders"
                        )

                url = payload.get("@odata.nextLink")

        return None

    def _fetch_emails_from_folder_id(self, headers: dict[str, str], folder_id: str) -> list[dict[str, Any]]:
        """Fetch emails directly by folder ID, bypassing the LlamaIndex reader.

        Used as a fallback after _resolve_folder_id() resolves a custom folder
        display name to its Graph API folder ID.
        """
        response = requests.get(
            f"{self._user_mail_folders_url()}/mailFolders/{quote(folder_id, safe='')}/messages",
            headers=headers,
            params={"$top": self.num_mails},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json().get("value", [])

    def _user_mail_folders_url(self) -> str:
        """Base Graph API URL for this user's mailbox."""
        return f"{GRAPH_API_BASE}/users/{quote(self.user_email, safe='@')}"

    def get_raw_content(self, item: IngestionItem) -> str:
        """Format a raw Graph API email dict as Markdown."""
        email = item.source_ref
        subject = email.get("subject") or "(no subject)"
        sender = self._extract_sender(email)
        received = email.get("receivedDateTime", "")
        body = (email.get("body") or {}).get("content") or ""

        return f"# {subject}\n\n**From:** {sender}\n**Received:** {received}\n\n{body}"

    def get_item_name(self, item: IngestionItem) -> str:
        return f"outlook_{item.id.removeprefix('outlook:')}"[:255]

    def get_document_metadata(
        self,
        item: IngestionItem,
        item_name: str,
        checksum: str,
        version: int,
        last_modified: Any,
    ) -> dict[str, Any]:
        email = item.source_ref
        metadata = super().get_document_metadata(item, item_name, checksum, version, last_modified)
        metadata.update(
            {
                "user_email": self.user_email,
                "folder": self.folder,
                "subject": email.get("subject") or "",
                "sender": self._extract_sender(email),
                "received_at": email.get("receivedDateTime") or "",
            }
        )
        return metadata

    @staticmethod
    def _extract_sender(email: dict) -> str:
        try:
            return email["from"]["emailAddress"]["address"]
        except (KeyError, TypeError):
            return ""

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            normalized = value.replace("Z", "+00:00")
            # Graph API timestamps can have 7 fractional-second digits; fromisoformat
            # supports only up to 6, so truncate the excess digits before parsing.
            if "." in normalized:
                dot_pos = normalized.index(".")
                tz_pos = normalized.find("+", dot_pos)
                if tz_pos == -1:
                    tz_pos = normalized.find("-", dot_pos)
                frac = normalized[dot_pos + 1 : tz_pos] if tz_pos != -1 else normalized[dot_pos + 1 :]
                if len(frac) > 6:
                    tz_suffix = normalized[tz_pos:] if tz_pos != -1 else ""
                    normalized = normalized[: dot_pos + 1] + frac[:6] + tz_suffix
            dt = datetime.fromisoformat(normalized)
            return dt.astimezone(UTC)
        except (ValueError, TypeError):
            return None
