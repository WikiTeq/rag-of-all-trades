import logging
from collections import deque
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.http import RetrySession
from utils.parse import parse_bool
from utils.text import html_to_markdown

logger = logging.getLogger(__name__)

GRAPH_API_BASE = "https://graph.microsoft.com/v1.0"
TOKEN_URL_TEMPLATE = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
GRAPH_DEFAULT_SCOPE = "https://graph.microsoft.com/.default"

# Graph API well-known mail folder names that can be used directly in the
# /mailFolders/{name}/messages path without resolving a display name to an ID.
# https://learn.microsoft.com/en-us/graph/api/resources/mailfolder
WELL_KNOWN_FOLDERS = frozenset(
    {
        "archive",
        "clutter",
        "conflicts",
        "conversationhistory",
        "deleteditems",
        "drafts",
        "inbox",
        "junkemail",
        "localfailures",
        "msgfolderroot",
        "outbox",
        "recoverableitemsdeletions",
        "scheduled",
        "searchfolders",
        "sentitems",
        "serverfailures",
        "syncissues",
    }
)


class OutlookIngestionJob(IngestionJob):
    """Ingestion connector for Microsoft Outlook email via Microsoft Graph API.

    Authenticates directly against Microsoft Entra ID using the OAuth2 client
    credentials flow and fetches emails via the Microsoft Graph API.
    Requires an Azure app registration with Mail.Read application permission and
    admin consent. Only supported for Microsoft 365 / Entra ID work or school accounts
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

        self.html_to_text: bool = parse_bool(cfg.get("html_to_text"), default=True)
        self._resolved_folder_id: str | None = None
        self._session = RetrySession()

        logger.info(
            f"Initialized Outlook connector [{self.source_name}] (folder={self.folder!r}, num_mails={self.num_mails})"
        )

    def list_items(self) -> Iterator[IngestionItem]:
        """Fetch emails via the Microsoft Graph API and yield one IngestionItem per email."""
        logger.info(f"[{self.source_name}] Listing emails in folder {self.folder!r}")

        headers = self._get_auth_headers()
        emails = self._fetch_emails(headers)

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

    def _get_auth_headers(self) -> dict[str, str]:
        """Fetch a client-credentials access token from Entra ID and build Bearer headers.

        Uses the OAuth2 client credentials flow directly against Microsoft Entra ID
        (Azure AD) — https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow.
        """
        token_url = TOKEN_URL_TEMPLATE.format(tenant_id=self.tenant_id)
        response = self._session.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": GRAPH_DEFAULT_SCOPE,
            },
            retry=True,
        )
        response.raise_for_status()
        payload = response.json()

        access_token = payload.get("access_token")
        if not access_token:
            raise RuntimeError(f"[{self.source_name}] Entra ID token response did not include an access_token")

        return {"Authorization": f"Bearer {access_token}"}

    def _fetch_emails(self, headers: dict[str, str]) -> list[dict[str, Any]]:
        """Resolve the configured folder to a Graph API folder ID and fetch its messages.

        Well-known folder names (e.g. "Inbox", "SentItems") are accepted directly
        by the Graph API /mailFolders/{name}/messages path. Custom display names
        are not — they must first be resolved to a folder ID via _resolve_folder_id().
        """
        if self._resolved_folder_id:
            folder_key = self._resolved_folder_id
        elif self.folder.casefold() in WELL_KNOWN_FOLDERS:
            folder_key = self.folder
        else:
            folder_id = self._resolve_folder_id(headers)
            if not folder_id:
                raise ValueError(
                    f"[{self.source_name}] Outlook folder {self.folder!r} not found "
                    "(not a well-known folder name and no matching display name)"
                )

            logger.info(
                "[%s] Resolved Outlook folder display name %r to Graph folder id %r",
                self.source_name,
                self.folder,
                folder_id,
            )
            self._resolved_folder_id = folder_id
            folder_key = folder_id

        return self._fetch_emails_from_folder_id(headers, folder_key)

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
                response = self._session.get(
                    url,
                    headers=headers,
                    params={"$top": 100, "includeHiddenFolders": "true"} if is_first else None,
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

    def _fetch_emails_from_folder_id(self, headers: dict[str, str], folder_key: str) -> list[dict[str, Any]]:
        """Fetch messages from a Graph API mail folder, identified by name or ID.

        folder_key accepts either a well-known folder name (e.g. "Inbox") or a
        resolved folder ID from _resolve_folder_id() — both are valid at the
        Graph API /mailFolders/{folder_key}/messages path. Paginates via opaque
        @odata.nextLink until num_mails is reached or no further pages exist.
        """
        url: str | None = f"{self._user_mail_folders_url()}/mailFolders/{quote(folder_key, safe='')}/messages"
        params: dict | None = {"$top": self.num_mails}
        results: list[dict[str, Any]] = []

        while url and len(results) < self.num_mails:
            response = self._session.get(url, headers=headers, params=params)
            response.raise_for_status()
            payload = response.json()
            results.extend(payload.get("value", []))
            url = payload.get("@odata.nextLink")
            params = None

        return results[: self.num_mails]

    def _user_mail_folders_url(self) -> str:
        """Base Graph API URL for this user's mailbox."""
        return f"{GRAPH_API_BASE}/users/{quote(self.user_email, safe='@')}"

    def get_raw_content(self, item: IngestionItem) -> str:
        """Format a raw Graph API email dict as Markdown."""
        email = item.source_ref
        subject = email.get("subject") or "(no subject)"
        sender = self._extract_sender(email)
        received = email.get("receivedDateTime", "")
        body_obj = email.get("body") or {}
        body = body_obj.get("content") or ""
        content_type = body_obj.get("contentType", "text")
        # Graph returns contentType as "html" or "text"; only convert HTML bodies
        if content_type == "html" and self.html_to_text:
            body = html_to_markdown(body)

        return f"# {subject}\n\n**From:** {sender}\n**Received:** {received}\n\n{body}"

    def get_item_name(self, item: IngestionItem) -> str:
        return f"outlook_{item.id.removeprefix('outlook:')}"[:255]

    def get_item_checksum(self, item: IngestionItem) -> str | None:
        email = item.source_ref
        msg_id = email.get("id")
        received = email.get("receivedDateTime")
        if msg_id and received:
            return f"{msg_id}:{received}"
        return None

    def get_extra_metadata(self, item: IngestionItem, _content: str, _metadata: dict[str, Any]) -> dict[str, Any]:
        email = item.source_ref
        return {
            "user_email": self.user_email,
            "folder": self.folder,
            "subject": email.get("subject") or "",
            "sender": self._extract_sender(email),
            "received_at": email.get("receivedDateTime") or "",
            "web_link": email.get("webLink") or "",
        }

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
