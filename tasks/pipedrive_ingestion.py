# Standard library imports
import logging
import re
import time
from collections.abc import Iterator
from datetime import datetime
from typing import Any

# Third-party imports
import html2text
import requests

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)

# Pipedrive REST API base URL
_API_BASE = "https://api.pipedrive.com/v1"

# All supported entity types and their API endpoints
_ENTITY_ENDPOINTS: dict[str, str] = {
    "activities": "/activities",
    "deals": "/deals",
    "notes": "/notes",
    "organizations": "/organizations",
    "persons": "/persons",
    "products": "/products",
    "projects": "/projects",
    "leads": "/leads",
    "tasks": "/tasks",
    "mails": "/mailbox/mailMessages",
}

# Entity types that support the Pipedrive filter_id API param
_FILTERABLE_ENTITIES = {
    "activities",
    "deals",
    "organizations",
    "persons",
    "products",
    "projects",
    "leads",
}


class PipedriveClient:
    """Thin wrapper around the Pipedrive REST API v1.

    Handles authentication (api_token query param), pagination
    (start / more_items_in_collection), retries with backoff on
    429 / 5xx responses, and optional per-request delay.
    """

    def __init__(self, api_token: str, request_delay: float, max_retries: int):
        self._token = api_token
        self._delay = request_delay
        self._max_retries = max_retries
        self._session = requests.Session()
        self._session.params = {"api_token": api_token}  # type: ignore[assignment]

        # Caches for ID → name resolution
        self._user_cache: dict[int, str] = {}
        self._pipeline_cache: dict[int, str] = {}
        self._stage_cache: dict[int, str] = {}

    def get(self, path: str, params: dict | None = None) -> dict:
        """Perform a GET request with retry logic."""
        url = f"{_API_BASE}{path}"
        params = dict(params or {})

        for attempt in range(self._max_retries + 1):
            try:
                resp = self._session.get(url, params=params, timeout=30)
            except requests.RequestException as exc:
                if attempt < self._max_retries:
                    wait = 2**attempt
                    logger.warning(f"Request error ({exc}); retrying in {wait}s")
                    time.sleep(wait)
                    continue
                raise

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 2**attempt))
                logger.warning(f"Rate limited; waiting {retry_after}s")
                time.sleep(retry_after)
                continue

            if resp.status_code >= 500:
                if attempt < self._max_retries:
                    wait = 2**attempt
                    logger.warning(f"Server error {resp.status_code}; retrying in {wait}s")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()

            resp.raise_for_status()
            return resp.json()

        return {}

    def paginate(self, path: str, params: dict | None = None, limit: int | None = None) -> Iterator[dict]:
        """Yield all records from a paginated Pipedrive endpoint."""
        params = dict(params or {})
        params.setdefault("limit", 100)
        start = 0
        yielded = 0

        while True:
            params["start"] = start
            data = self.get(path, params)

            if not data.get("success"):
                logger.error(f"Pipedrive API error for {path}: {data.get('error')}")
                break

            records = data.get("data") or []
            if not records:
                break

            for record in records:
                yield record
                yielded += 1
                if limit is not None and yielded >= limit:
                    return

            pagination = (data.get("additional_data") or {}).get("pagination") or {}
            if not pagination.get("more_items_in_collection"):
                break

            start = pagination.get("next_start", start + len(records))

            if self._delay > 0:
                time.sleep(self._delay)

    def resolve_user(self, user_id: int | None) -> str:
        """Resolve a user ID to a display name, with caching."""
        if user_id is None:
            return ""
        if user_id in self._user_cache:
            return self._user_cache[user_id]
        try:
            data = self.get(f"/users/{user_id}")
            name = (data.get("data") or {}).get("name", "") or ""
        except Exception as exc:
            logger.warning(f"Failed to resolve user {user_id}: {exc}")
            name = str(user_id)
        self._user_cache[user_id] = name
        return name

    def resolve_pipeline(self, pipeline_id: int | None) -> str:
        """Resolve a pipeline ID to its name, with caching."""
        if pipeline_id is None:
            return ""
        if pipeline_id in self._pipeline_cache:
            return self._pipeline_cache[pipeline_id]
        try:
            data = self.get(f"/pipelines/{pipeline_id}")
            name = (data.get("data") or {}).get("name", "") or ""
        except Exception as exc:
            logger.warning(f"Failed to resolve pipeline {pipeline_id}: {exc}")
            name = str(pipeline_id)
        self._pipeline_cache[pipeline_id] = name
        return name

    def resolve_stage(self, stage_id: int | None) -> str:
        """Resolve a stage ID to its name, with caching."""
        if stage_id is None:
            return ""
        if stage_id in self._stage_cache:
            return self._stage_cache[stage_id]
        try:
            data = self.get(f"/stages/{stage_id}")
            name = (data.get("data") or {}).get("name", "") or ""
        except Exception as exc:
            logger.warning(f"Failed to resolve stage {stage_id}: {exc}")
            name = str(stage_id)
        self._stage_cache[stage_id] = name
        return name


class PipedriveIngestionJob(IngestionJob):
    """Ingestion connector for Pipedrive CRM.

    Fetches CRM records from the Pipedrive REST API v1 and stores them in the
    vector store. Supports activities, deals, notes, organizations, persons,
    products, projects, leads, tasks, and mails.

    Configuration (config.yaml):
        - config.api_token: Pipedrive API token (required)
        - config.load_types: list of entity types to ingest (optional, default: all)
        - config.max_items: global per-entity fetch limit (optional, default: unlimited)
        - config.request_delay: seconds between API requests (optional, default: 0)
        - config.max_retries: retry count for failed/rate-limited requests (optional, default: 3)
        - config.filter_mail_folders: mail folders to include (optional, default: [inbox])
        - config.filter_activities_updated_since: ISO date string (optional)
        - config.filter_deals_updated_since: ISO date string (optional)
        - config.filter_deals_stages_ids: list of stage IDs to include (optional)
        - config.filter_activities_filter_id: Pipedrive filter ID for activities (optional)
        - config.filter_deals_filter_id: Pipedrive filter ID for deals (optional)
        - config.filter_organizations_filter_id: Pipedrive filter ID for organizations (optional)
        - config.filter_persons_filter_id: Pipedrive filter ID for persons (optional)
        - config.filter_products_filter_id: Pipedrive filter ID for products (optional)
        - config.filter_projects_filter_id: Pipedrive filter ID for projects (optional)
        - config.filter_leads_filter_id: Pipedrive filter ID for leads (optional)
    """

    @property
    def source_type(self) -> str:
        return "pipedrive"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        self.api_token = cfg.get("api_token", "").strip()
        if not self.api_token:
            raise ValueError("api_token is required in Pipedrive connector config")

        # Entity types to load
        raw_types = cfg.get("load_types") or list(_ENTITY_ENDPOINTS.keys())
        self.load_types: list[str] = [t.strip() for t in raw_types if t.strip()]
        unknown = set(self.load_types) - set(_ENTITY_ENDPOINTS)
        if unknown:
            raise ValueError(f"Unknown load_types: {unknown}. Valid: {list(_ENTITY_ENDPOINTS)}")

        # Global per-entity limit
        raw_max = cfg.get("max_items")
        self.max_items: int | None = int(raw_max) if raw_max is not None else None

        self.request_delay = float(cfg.get("request_delay", 0.0))
        self.max_retries = int(cfg.get("max_retries", 3))

        # Mail folder filter
        self.filter_mail_folders: list[str] = cfg.get("filter_mail_folders") or ["inbox"]

        # Date filters
        self.filter_activities_updated_since: str | None = cfg.get("filter_activities_updated_since")
        self.filter_deals_updated_since: str | None = cfg.get("filter_deals_updated_since")

        # Stage filter for deals
        self.filter_deals_stages_ids: list[str] = cfg.get("filter_deals_stages_ids") or []

        # Per-entity filter IDs
        self.filter_ids: dict[str, str | None] = {
            entity: cfg.get(f"filter_{entity}_filter_id") for entity in _FILTERABLE_ENTITIES
        }

        self._client = PipedriveClient(
            api_token=self.api_token,
            request_delay=self.request_delay,
            max_retries=self.max_retries,
        )

        logger.info(f"Initialized Pipedrive connector (load_types={self.load_types}, max_items={self.max_items})")

    def list_items(self) -> Iterator[IngestionItem]:
        """Yield one IngestionItem per Pipedrive record across all configured entity types."""
        for entity_type in self.load_types:
            logger.info(f"[{self.source_name}] Listing {entity_type}")
            endpoint = _ENTITY_ENDPOINTS[entity_type]
            base_params = self._build_list_params(entity_type)

            # For mails, the Pipedrive API only accepts one folder_id per request;
            # iterate over each configured folder separately.
            if entity_type == "mails" and self.filter_mail_folders:
                _folder_map = {"inbox": 1, "drafts": 2, "sent": 3, "archive": 4, "spam": 5, "trash": 6}
                folder_ids = [str(_folder_map[f]) for f in self.filter_mail_folders if f in _folder_map]
            else:
                folder_ids = [None]  # sentinel: run once without a folder_id override

            seen_ids: set = set()
            count = 0

            for folder_id in folder_ids:
                params = dict(base_params)
                if folder_id is not None:
                    params["folder_id"] = folder_id

                try:
                    for record in self._client.paginate(endpoint, params=params, limit=self.max_items):
                        if self.max_items is not None and count >= self.max_items:
                            break
                        # Client-side stage filter for multi-stage deal configs
                        if entity_type == "deals" and len(self.filter_deals_stages_ids) > 1:
                            if str(record.get("stage_id", "")) not in [str(s) for s in self.filter_deals_stages_ids]:
                                continue
                        record_id = record.get("id") or record.get("cc_email")
                        if record_id in seen_ids:
                            continue
                        seen_ids.add(record_id)
                        updated_at = self._parse_timestamp(
                            record.get("update_time") or record.get("updated_at") or record.get("add_time")
                        )
                        yield IngestionItem(
                            id=f"pipedrive:{entity_type}:{record_id}",
                            source_ref={"type": entity_type, "data": record},
                            last_modified=updated_at,
                        )
                        count += 1
                    if self.max_items is not None and count >= self.max_items:
                        break
                except Exception as exc:
                    logger.error(f"[{self.source_name}] Failed to list {entity_type}: {exc}")

    def get_raw_content(self, item: IngestionItem) -> str:
        """Build Markdown-formatted content for a Pipedrive record."""
        entity_type = item.source_ref["type"]
        record = item.source_ref["data"]

        builder = getattr(self, f"_build_{entity_type}_content", self._build_generic_content)
        content = builder(record)

        # Cache the record URL for metadata
        item._metadata_cache["record_url"] = self._build_record_url(entity_type, record)
        item._metadata_cache["entity_type"] = entity_type

        return content

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe identifier: ``pipedrive_{type}_{id}``."""
        entity_type = item.source_ref["type"]
        record = item.source_ref["data"]
        record_id = record.get("id") or re.sub(r"[^\w]", "_", str(record.get("cc_email", "")))
        safe = re.sub(r"[^\w\-]", "_", f"pipedrive_{entity_type}_{record_id}")
        return safe[:255]

    def get_document_metadata(
        self,
        item: IngestionItem,
        item_name: str,
        checksum: str,
        version: int,
        last_modified: Any,
    ) -> dict[str, Any]:
        """Build metadata dict with Pipedrive-specific fields."""
        record = item.source_ref["data"]
        entity_type = item.source_ref["type"]

        metadata = super().get_document_metadata(item, item_name, checksum, version, last_modified)
        metadata.update(
            {
                "entity_type": entity_type,
                "pipedrive_id": str(record.get("id", "")),
                "title": self._record_title(entity_type, record),
                "url": item._metadata_cache.get("record_url", ""),
                "add_time": record.get("add_time", "") or "",
                "update_time": record.get("update_time") or record.get("updated_at", "") or "",
            }
        )

        # Entity-specific metadata extensions
        extender = getattr(self, f"_extend_{entity_type}_metadata", None)
        if extender:
            metadata.update(extender(record))

        return metadata

    def _build_deals_content(self, record: dict) -> str:
        """Build Markdown content for a Deal record.

        Resolves pipeline and stage IDs to human-readable names via the API cache.
        """
        parts: list[str] = []
        title = record.get("title", "") or ""
        parts.append(f"# Deal: {title}\n")

        pipeline_name = self._client.resolve_pipeline(record.get("pipeline_id"))
        stage_name = self._client.resolve_stage(record.get("stage_id"))
        if pipeline_name:
            parts.append(f"**Pipeline:** {pipeline_name}")
        if stage_name:
            parts.append(f"**Stage:** {stage_name}")

        status = record.get("status", "") or ""
        if status:
            parts.append(f"**Status:** {status}")

        value = record.get("value")
        currency = record.get("currency", "") or ""
        if value is not None:
            parts.append(f"**Value:** {value} {currency}".strip())

        owner = record.get("owner_name") or self._client.resolve_user(record.get("owner_id") or record.get("user_id"))
        if owner:
            parts.append(f"**Owner:** {owner}")

        org_name = record.get("org_name", "") or ""
        person_name = record.get("person_name", "") or ""
        if org_name:
            parts.append(f"**Organization:** {org_name}")
        if person_name:
            parts.append(f"**Contact:** {person_name}")

        close_time = record.get("close_time") or record.get("expected_close_date", "") or ""
        if close_time:
            parts.append(f"**Expected Close:** {close_time}")

        return "\n".join(parts)

    def _build_notes_content(self, record: dict) -> str:
        """Build Markdown content for a Note record.

        Appends any comments on the note fetched via a separate API call.
        """
        parts: list[str] = []
        content = record.get("content", "") or ""
        parts.append("# Note\n")
        if content.strip():
            parts.append(content)

        # Fetch and append comments
        note_id = record.get("id")
        if note_id:
            comments_md = self._fetch_note_comments(note_id)
            if comments_md:
                parts.append(comments_md)

        return "\n\n".join(parts)

    def _build_activities_content(self, record: dict) -> str:
        """Build Markdown content for an Activity record.

        The activity note field may contain HTML (e.g. from Pipedrive's rich-text
        editor), so it is converted to plain text via html2text before embedding.
        """
        parts: list[str] = []
        subject = record.get("subject", "") or ""
        parts.append(f"# Activity: {subject}\n")

        activity_type = record.get("type", "") or ""
        if activity_type:
            parts.append(f"**Type:** {activity_type}")

        due_date = record.get("due_date", "") or ""
        due_time = record.get("due_time", "") or ""
        if due_date:
            parts.append(f"**Due:** {due_date} {due_time}".strip())

        done = record.get("done")
        parts.append(f"**Done:** {'Yes' if done else 'No'}")

        note = record.get("note", "") or ""
        if note.strip():
            h = html2text.HTML2Text()
            h.ignore_links = True
            h.ignore_images = True
            parts.append(f"\n{h.handle(note).strip()}")

        user_id = record.get("user_id") or record.get("assigned_to_user_id")
        owner = record.get("owner_name") or self._client.resolve_user(user_id)
        if owner:
            parts.append(f"**Assigned to:** {owner}")

        deal_title = record.get("deal_title", "") or ""
        org_name = record.get("org_name", "") or ""
        person_name = record.get("person_name", "") or ""
        if deal_title:
            parts.append(f"**Deal:** {deal_title}")
        if org_name:
            parts.append(f"**Organization:** {org_name}")
        if person_name:
            parts.append(f"**Person:** {person_name}")

        return "\n".join(parts)

    def _build_persons_content(self, record: dict) -> str:
        """Build Markdown content for a Person record.

        Emails and phones are returned as lists of objects by the API, so each
        entry is iterated to extract its ``value`` field.
        """
        parts: list[str] = []
        name = record.get("name", "") or ""
        parts.append(f"# Person: {name}\n")

        org_name = record.get("org_name") or (record.get("org_id") or {}).get("name", "") or ""
        if org_name:
            parts.append(f"**Organization:** {org_name}")

        emails = record.get("email") or []
        if isinstance(emails, list):
            for e in emails:
                if e.get("value"):
                    parts.append(f"**Email:** {e['value']}")
        phones = record.get("phone") or []
        if isinstance(phones, list):
            for p in phones:
                if p.get("value"):
                    parts.append(f"**Phone:** {p['value']}")

        owner = record.get("owner_name") or self._client.resolve_user(record.get("owner_id"))
        if owner:
            parts.append(f"**Owner:** {owner}")

        return "\n".join(parts)

    def _build_organizations_content(self, record: dict) -> str:
        """Build Markdown content for an Organization record."""
        parts: list[str] = []
        name = record.get("name", "") or ""
        parts.append(f"# Organization: {name}\n")

        address = record.get("address", "") or ""
        if address:
            parts.append(f"**Address:** {address}")

        owner = record.get("owner_name") or self._client.resolve_user(record.get("owner_id"))
        if owner:
            parts.append(f"**Owner:** {owner}")

        return "\n".join(parts)

    def _build_products_content(self, record: dict) -> str:
        """Build Markdown content for a Product record.

        Prices are capped at 3 entries to avoid overwhelming the embedding context.
        """
        parts: list[str] = []
        name = record.get("name", "") or ""
        parts.append(f"# Product: {name}\n")

        code = record.get("code", "") or ""
        if code:
            parts.append(f"**Code:** {code}")

        description = record.get("description", "") or ""
        if description.strip():
            parts.append(f"\n{description}")

        price_list = record.get("prices") or []
        for price_entry in price_list[:3]:
            currency = price_entry.get("currency", "")
            price = price_entry.get("price")
            if price is not None:
                parts.append(f"**Price ({currency}):** {price}")

        return "\n".join(parts)

    def _build_projects_content(self, record: dict) -> str:
        """Build Markdown content for a Project record."""
        parts: list[str] = []
        title = record.get("title", "") or ""
        parts.append(f"# Project: {title}\n")

        status = record.get("status", "") or ""
        if status:
            parts.append(f"**Status:** {status}")

        description = record.get("description", "") or ""
        if description.strip():
            parts.append(f"\n{description}")

        owner_id = record.get("owner_id")
        owner = self._client.resolve_user(owner_id)
        if owner:
            parts.append(f"**Owner:** {owner}")

        return "\n".join(parts)

    def _build_leads_content(self, record: dict) -> str:
        """Build Markdown content for a Lead record.

        Organization and person are nested objects in the Leads API response,
        unlike other entity types where they appear as flat name fields.
        """
        parts: list[str] = []
        title = record.get("title", "") or ""
        parts.append(f"# Lead: {title}\n")

        owner_id = record.get("owner_id")
        owner = self._client.resolve_user(owner_id)
        if owner:
            parts.append(f"**Owner:** {owner}")

        org_name = (record.get("organization") or {}).get("name", "") or ""
        person_name = (record.get("person") or {}).get("name", "") or ""
        if org_name:
            parts.append(f"**Organization:** {org_name}")
        if person_name:
            parts.append(f"**Contact:** {person_name}")

        return "\n".join(parts)

    def _build_tasks_content(self, record: dict) -> str:
        """Build Markdown content for a Task record.

        The assignee_id is resolved to a name via the API since the Tasks endpoint
        does not return the assignee name directly.
        """
        parts: list[str] = []
        title = record.get("title", "") or ""
        parts.append(f"# Task: {title}\n")

        assignee_id = record.get("assignee_id")
        assignee = self._client.resolve_user(assignee_id)
        if assignee:
            parts.append(f"**Assigned to:** {assignee}")

        due_date = record.get("due_date", "") or ""
        if due_date:
            parts.append(f"**Due:** {due_date}")

        done = record.get("done")
        parts.append(f"**Done:** {'Yes' if done else 'No'}")

        description = record.get("description", "") or ""
        if description.strip():
            parts.append(f"\n{description}")

        return "\n".join(parts)

    def _build_mails_content(self, record: dict) -> str:
        """Build Markdown content for a Mail (email) record.

        Prefers body_text over body_html. Falls back to body_html with basic tag
        stripping if body_text is unavailable, then to the snippet field.
        """
        parts: list[str] = []
        subject = record.get("subject", "") or ""
        parts.append(f"# Mail: {subject}\n")

        from_addr = record.get("from") or []
        if isinstance(from_addr, list):
            for f in from_addr:
                name = f.get("name") or f.get("email_address", "")
                if name:
                    parts.append(f"**From:** {name}")

        to_addr = record.get("to") or []
        if isinstance(to_addr, list):
            recipients = [
                t.get("name") or t.get("email_address", "") for t in to_addr if t.get("name") or t.get("email_address")
            ]
            if recipients:
                parts.append(f"**To:** {', '.join(recipients)}")

        body = record.get("body_text") or record.get("body_html") or record.get("snippet", "") or ""
        # Strip basic HTML tags from body_html if body_text is not available
        if body and record.get("body_html") and not record.get("body_text"):
            body = re.sub(r"<[^>]+>", " ", body)
            body = re.sub(r"\s+", " ", body).strip()
        if body.strip():
            parts.append(f"\n{body}")

        return "\n\n".join(parts)

    def _build_generic_content(self, record: dict) -> str:
        """Fallback: render all string/number fields as a Markdown list."""
        lines: list[str] = ["# Record\n"]
        for key, value in record.items():
            if isinstance(value, str | int | float | bool) and value not in (None, ""):
                lines.append(f"- **{key}:** {value}")
        return "\n".join(lines)

    def _extend_deals_metadata(self, record: dict) -> dict:
        """Return Deal-specific metadata fields for the vector store."""
        return {
            "pipeline": self._client.resolve_pipeline(record.get("pipeline_id")),
            "stage": self._client.resolve_stage(record.get("stage_id")),
            "status": record.get("status", "") or "",
            "owner": record.get("owner_name") or self._client.resolve_user(record.get("user_id")),
            "org_name": record.get("org_name", "") or "",
            "person_name": record.get("person_name", "") or "",
            "value": str(record.get("value", "") or ""),
            "currency": record.get("currency", "") or "",
        }

    def _extend_notes_metadata(self, record: dict) -> dict:
        """Return Note-specific metadata fields including author and linked entity IDs."""
        author_id = (record.get("user") or {}).get("id") or record.get("user_id")
        return {
            "author": (record.get("user") or {}).get("name") or self._client.resolve_user(author_id),
            "linked_deal_id": str(record.get("deal_id") or ""),
            "linked_org_id": str(record.get("org_id") or ""),
            "linked_person_id": str(record.get("person_id") or ""),
            "linked_project_id": str(record.get("project_id") or ""),
            "activity_flag": str(record.get("active_flag", "") or ""),
        }

    def _extend_activities_metadata(self, record: dict) -> dict:
        """Return Activity-specific metadata fields for the vector store."""
        user_id = record.get("user_id") or record.get("assigned_to_user_id")
        return {
            "activity_type": record.get("type", "") or "",
            "done": str(record.get("done", "") or ""),
            "due_date": record.get("due_date", "") or "",
            "assigned_to": record.get("owner_name") or self._client.resolve_user(user_id),
            "deal_title": record.get("deal_title", "") or "",
            "org_name": record.get("org_name", "") or "",
            "person_name": record.get("person_name", "") or "",
        }

    def _extend_persons_metadata(self, record: dict) -> dict:
        """Return Person-specific metadata fields for the vector store."""
        org = record.get("org_id") or {}
        return {
            "org_name": record.get("org_name") or (org.get("name") if isinstance(org, dict) else "") or "",
            "owner": record.get("owner_name") or self._client.resolve_user(record.get("owner_id")),
        }

    def _extend_organizations_metadata(self, record: dict) -> dict:
        """Return Organization-specific metadata fields for the vector store."""
        return {
            "address": record.get("address", "") or "",
            "owner": record.get("owner_name") or self._client.resolve_user(record.get("owner_id")),
        }

    def _extend_tasks_metadata(self, record: dict) -> dict:
        """Return Task-specific metadata fields for the vector store."""
        return {
            "assignee": self._client.resolve_user(record.get("assignee_id")),
            "due_date": record.get("due_date", "") or "",
            "done": str(record.get("done", "") or ""),
        }

    def _build_list_params(self, entity_type: str) -> dict:
        """Build query params for the list endpoint based on config filters."""
        params: dict = {}

        # If a filter_id is set for this entity, use it (takes priority)
        filter_id = self.filter_ids.get(entity_type)
        if filter_id and entity_type in _FILTERABLE_ENTITIES:
            params["filter_id"] = filter_id
            return params

        if entity_type == "activities":
            if self.filter_activities_updated_since:
                params["updated_since"] = self.filter_activities_updated_since

        elif entity_type == "deals":
            if self.filter_deals_updated_since:
                params["updated_since"] = self.filter_deals_updated_since
            if self.filter_deals_stages_ids:
                # Single stage: push to API. Multiple stages: list_items handles client-side filtering.
                if len(self.filter_deals_stages_ids) == 1:
                    params["stage_id"] = self.filter_deals_stages_ids[0]

        # folder_id for mails is injected per-folder in list_items.

        return params

    def _fetch_note_comments(self, note_id: int) -> str:
        """Fetch comments for a note and return them as a Markdown string."""
        try:
            data = self._client.get(f"/notes/{note_id}/comments")
            comments = data.get("data") or []
        except Exception as exc:
            logger.warning(f"[{self.source_name}] Failed to fetch comments for note {note_id}: {exc}")
            return ""

        if not comments:
            return ""

        lines: list[str] = ["## Comments"]
        for comment in comments:
            user_name = (comment.get("user") or {}).get("name") or ""
            add_time = comment.get("add_time", "") or ""
            content = comment.get("content", "") or ""
            lines.append(f"**{user_name}** ({add_time}):\n{content}")

        return "\n\n".join(lines)

    def _build_record_url(self, entity_type: str, record: dict) -> str:
        """Build a Pipedrive app URL for the given record."""
        record_id = record.get("id")
        if not record_id:
            return ""
        _url_paths = {
            "deals": "deal",
            "persons": "person",
            "organizations": "organization",
            "activities": "activity",
            "leads": "lead",
            "products": "product",
            "projects": "project",
        }
        path = _url_paths.get(entity_type)
        if path:
            return f"https://app.pipedrive.com/{path}/{record_id}/detail"
        return ""

    def _record_title(self, entity_type: str, record: dict) -> str:
        """Extract a human-readable title from a record."""
        for key in ("title", "subject", "name", "content"):
            val = record.get(key)
            if val and isinstance(val, str):
                return val[:120]
        return f"{entity_type}:{record.get('id', '')}"

    @staticmethod
    def _parse_timestamp(value: str | None) -> datetime | None:
        """Parse a Pipedrive ISO-8601 timestamp string into a datetime object."""
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            return None
