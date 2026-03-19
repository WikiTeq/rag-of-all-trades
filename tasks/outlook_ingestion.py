import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from llama_index.readers.microsoft_outlook_emails import OutlookEmailReader

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)


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
        - config.schedules: Celery schedule in seconds (optional)
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

        self.folder = cfg.get("folder", "Inbox")
        self.num_mails = int(cfg.get("num_mails", 10))
        if self.num_mails <= 0:
            raise ValueError("num_mails must be positive in Outlook connector config")

        logger.info(
            f"Initialized Outlook connector for {self.user_email} (folder={self.folder!r}, num_mails={self.num_mails})"
        )

    def list_items(self) -> Iterator[IngestionItem]:
        """Fetch emails via OutlookEmailReader and yield one IngestionItem per email.

        Uses the reader's internal ``_fetch_emails()`` to obtain raw Graph API
        email dicts (including stable message IDs and timestamps) since
        ``load_data()`` returns only plain-text strings with no IDs.
        """
        logger.info(f"[{self.source_name}] Listing emails in folder {self.folder!r}")

        reader = OutlookEmailReader(
            client_id=self.client_id,
            client_secret=self.client_secret,
            tenant_id=self.tenant_id,
            user_email=self.user_email,
            folder=self.folder,
            num_mails=self.num_mails,
        )
        reader._ensure_token()
        emails = reader._fetch_emails()

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

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

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
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.astimezone(UTC)
        except (ValueError, TypeError):
            return None
