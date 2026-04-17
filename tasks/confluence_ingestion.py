# Standard library imports
import logging
import re
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

# Third-party imports
from llama_index.readers.confluence import ConfluenceReader

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)

# Discovery modes — exactly one must be set per source config
_DISCOVERY_MODES = ("space_key", "page_ids", "page_label", "cql", "folder_id")


class ConfluenceIngestionJob(IngestionJob):
    """Ingestion connector for Atlassian Confluence Cloud and Server.

    Delegates all page discovery and content fetching to the LlamaIndex
    ``ConfluenceReader``.  The connector's only responsibility is to map
    config keys to reader arguments, iterate the returned ``Document``
    objects, and surface them to the base ``IngestionJob`` pipeline.

    Supported auth (mutually exclusive):
        - ``api_token`` alone  — Cloud token auth (recommended for Cloud)
        - ``username`` + ``password``  — Basic auth (Server / Data Center)
        - ``username`` + ``api_token``  — Token-based basic auth (Cloud)

    Discovery modes (exactly one required):
        - ``space_key``   — all pages in a space
        - ``page_ids``    — comma-separated list of page IDs
        - ``page_label``  — all pages with a given label
        - ``cql``         — arbitrary CQL query
        - ``folder_id``   — all pages inside a folder

    Configuration (config.yaml):
        - config.base_url: Confluence base URL, e.g. https://yoursite.atlassian.net/wiki (required)
        - config.api_token: Confluence API token (mutually exclusive with password)
        - config.username: Confluence username / email (optional)
        - config.password: Confluence password (mutually exclusive with api_token)
        - config.cloud: true for Cloud, false for Server/Data Center (default true)
        - config.space_key: Load all pages from a space
        - config.page_ids: Comma-separated page IDs to load
        - config.page_label: Load all pages with this label
        - config.cql: CQL query to select pages
        - config.folder_id: Load all pages inside this folder
        - config.page_status: Filter by page status, e.g. "current" (space_key mode only)
        - config.include_children: Also load descendant pages (page_ids mode only, default false)
        - config.max_pages: Maximum pages to load (default 50)
    """

    @property
    def source_type(self) -> str:
        return "confluence"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        self.base_url = cfg.get("base_url", "").rstrip("/")
        if not self.base_url:
            raise ValueError("base_url is required in Confluence connector config")

        self.api_token: str | None = cfg.get("api_token", "").strip() or None
        self.username: str | None = cfg.get("username", "").strip() or None
        self.password: str | None = cfg.get("password", "").strip() or None

        if not self.api_token and not self.password:
            raise ValueError("Confluence connector config requires either api_token or username+password")
        if self.api_token and self.password:
            raise ValueError("api_token and password are mutually exclusive in Confluence connector config")
        if self.password and not self.username:
            raise ValueError("username is required when using password authentication")

        self.cloud: bool = bool(cfg.get("cloud", True))

        active_modes = [m for m in _DISCOVERY_MODES if cfg.get(m)]
        if len(active_modes) != 1:
            raise ValueError(
                f"Exactly one of {_DISCOVERY_MODES} must be set in Confluence connector config; "
                f"got: {active_modes or 'none'}"
            )
        self._mode = active_modes[0]

        self.space_key: str | None = cfg.get("space_key") or None
        raw_page_ids = cfg.get("page_ids")
        self.page_ids: list[str] | None = self.parse_page_ids(raw_page_ids)
        self.page_label: str | None = cfg.get("page_label") or None
        self.cql: str | None = cfg.get("cql") or None
        self.folder_id: str | None = str(cfg["folder_id"]) if cfg.get("folder_id") else None

        self.page_status: str | None = cfg.get("page_status") or None
        self.include_children: bool = bool(cfg.get("include_children", False))
        self.max_pages: int = int(cfg.get("max_pages", 50))
        if self.max_pages <= 0:
            raise ValueError("max_pages must be positive")

        self._reader = self._build_reader()
        logger.info(
            f"Initialized Confluence connector for {self.base_url} "
            f"(mode={self._mode}, max_pages={self.max_pages}, cloud={self.cloud})"
        )

    def list_items(self) -> Iterator[IngestionItem]:
        """Load Confluence pages via ConfluenceReader and yield one IngestionItem per page."""
        logger.info(f"[{self.source_name}] Loading Confluence pages (mode={self._mode})")

        try:
            documents = self._reader.load_data(**self._build_load_data_kwargs())
        except Exception as e:
            logger.exception(f"[{self.source_name}] Failed to load Confluence pages: {e}")
            return

        logger.info(f"[{self.source_name}] Loaded {len(documents)} page(s)")

        for doc in documents:
            page_id = doc.metadata.get("page_id") or doc.metadata.get("id", "")
            last_modified = self._fetch_last_modified(page_id)
            yield IngestionItem(
                id=f"confluence:{page_id}",
                source_ref=doc,
                last_modified=last_modified,
            )

    def get_raw_content(self, item: IngestionItem) -> str:
        """Return the page text produced by ConfluenceReader."""
        doc = item.source_ref
        return doc.text or ""

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe identifier for the page."""
        doc = item.source_ref
        title = doc.metadata.get("title", "") or ""
        page_id = doc.metadata.get("page_id") or doc.metadata.get("id", "")
        raw = f"confluence_{page_id}_{title}" if title else f"confluence_{page_id}"
        safe = re.sub(r"[^\w\-]", "_", raw)
        return safe[:255]

    def get_extra_metadata(self, item: IngestionItem, _content: str, _metadata: dict[str, Any]) -> dict[str, Any]:
        doc = item.source_ref
        return {
            "url": doc.metadata.get("url", "") or "",
            "title": doc.metadata.get("title", "") or "",
            "page_id": doc.metadata.get("page_id") or doc.metadata.get("id", "") or "",
            "space_key": doc.metadata.get("space_key", "") or "",
        }

    def _fetch_last_modified(self, page_id: str) -> datetime:
        """Fetch the last-modified timestamp for a page via the Confluence REST API.

        Uses the already-authenticated client inside the reader so no extra credentials
        are needed. Falls back to the current UTC time if the call fails or the field
        is absent (ConfluenceReader does not include version info in its expand params).
        """
        try:
            page = self._reader.confluence.get_page_by_id(page_id, expand="version")
            when_str = page.get("version", {}).get("when")
            if when_str:
                return datetime.fromisoformat(when_str.replace("Z", "+00:00"))
        except Exception as e:
            logger.warning(f"[{self.source_name}] Could not fetch version.when for page {page_id}: {e}")
        return datetime.now(UTC)

    def _build_reader(self) -> ConfluenceReader:
        """Construct an authenticated ConfluenceReader.

        ConfluenceReader auth behaviour:
        - ``api_token`` alone  → Bearer token (Server/DC PAT)
        - ``user_name`` + ``password`` → Basic auth (Cloud or Server)

        For Confluence Cloud, the API token must be used as the password alongside
        the username (basic auth). When ``username`` is provided alongside
        ``api_token``, we pass the token as ``password`` to trigger basic auth.
        """
        if self.username:
            # Basic auth: Cloud (email + api_token as password) or Server (user + password)
            password = self.password or self.api_token
            return ConfluenceReader(
                base_url=self.base_url,
                cloud=self.cloud,
                user_name=self.username,
                password=password,
            )
        # Bearer token: Server/DC PAT without username
        return ConfluenceReader(
            base_url=self.base_url,
            cloud=self.cloud,
            api_token=self.api_token,
        )

    def _build_load_data_kwargs(self) -> dict:
        """Build the kwargs dict for ConfluenceReader.load_data()."""
        mode_kwargs: dict = {
            "space_key": {"space_key": self.space_key},
            "page_ids": {"page_ids": self.page_ids, "include_children": self.include_children},
            "page_label": {"label": self.page_label},
            "cql": {"cql": self.cql},
            "folder_id": {"folder_id": self.folder_id},
        }
        kwargs: dict = {"max_num_results": self.max_pages, **mode_kwargs[self._mode]}

        if self._mode == "space_key" and self.page_status:
            kwargs["page_status"] = self.page_status

        return kwargs

    @staticmethod
    def parse_page_ids(value: Any) -> list[str] | None:
        """Normalize page_ids to a list of strings, or None."""
        if not value:
            return None
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        return [v.strip() for v in str(value).split(",") if v.strip()]
