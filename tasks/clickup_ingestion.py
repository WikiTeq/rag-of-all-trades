import logging
import time
from collections import deque
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.http import RetrySession
from utils.parse import parse_bool, parse_list
from utils.text import slugify

logger = logging.getLogger(__name__)

CLICKUP_API_BASE_URL = "https://api.clickup.com/api/v2"
SCOPE_FILTERS = {"space": "space_ids[]", "folder": "project_ids[]", "list": "list_ids[]"}
TASKS_PAGE_SIZE = 100
COMMENTS_PAGE_SIZE = 25
DATE_FIELDS = ("date_created", "date_updated", "date_closed", "date_done", "due_date", "start_date")


class ClickUpIngestionJob(IngestionJob):
    """Ingestion connector for ClickUp tasks.

    Fetches tasks (with optional comments) from a ClickUp Workspace using the
    ClickUp API v2 directly, and stores them as Markdown in the vector store.

    Configuration (config.yaml):
        - config.api_token: ClickUp personal API token (required)
        - config.team_id: ClickUp Workspace (team) ID (required)
        - config.connector_type: Scope of the ingestion: ``workspace`` (default),
          ``space``, ``folder`` or ``list``
        - config.connector_ids: Comma-separated space, folder or list IDs
          (required when connector_type is not ``workspace``)
        - config.retrieve_task_comments: Whether to include task comments (optional, default true)
        - config.max_retries: Retry count for failed or rate-limited requests (optional, default 3)
        - config.max_requests_per_minute: Client-side request limit (optional, default 100,
          the limit of the Free, Unlimited and Business plans)
    """

    @property
    def source_type(self) -> str:
        return "clickup"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        self.api_token = str(cfg.get("api_token") or "").strip()
        if not self.api_token:
            raise ValueError("api_token is required in ClickUp connector config")

        self.team_id = str(cfg.get("team_id") or "").strip()
        if not self.team_id:
            raise ValueError("team_id is required in ClickUp connector config")

        self.connector_type = str(cfg.get("connector_type") or "workspace").strip().lower()
        if self.connector_type != "workspace" and self.connector_type not in SCOPE_FILTERS:
            raise ValueError("connector_type must be one of: workspace, space, folder, list")

        self.connector_ids: list[str] = parse_list(cfg.get("connector_ids"))
        if self.connector_type != "workspace" and not self.connector_ids:
            raise ValueError(f"connector_ids is required when connector_type is {self.connector_type!r}")

        self.retrieve_task_comments = parse_bool(cfg.get("retrieve_task_comments"), default=True)

        max_retries = int(cfg.get("max_retries", 3))
        if max_retries < 0:
            raise ValueError("max_retries must not be negative")

        self.max_requests_per_minute = int(cfg.get("max_requests_per_minute", 100))
        if self.max_requests_per_minute <= 0:
            raise ValueError("max_requests_per_minute must be positive")

        self._request_times: deque[float] = deque()
        self._session = RetrySession(max_retries=max_retries)
        self._headers = {"Authorization": self.api_token}

        logger.info(
            f"Initialized ClickUp connector (team_id={self.team_id}, connector_type={self.connector_type}, "
            f"connector_ids={self.connector_ids or 'all'}, retrieve_task_comments={self.retrieve_task_comments})"
        )

    def list_items(self) -> Iterator[IngestionItem]:
        """Yield one IngestionItem per ClickUp task in the configured scope."""
        seen: set[str] = set()
        params: dict[str, Any] = {"include_markdown_description": "true", "include_closed": "true"}
        if self.connector_type in SCOPE_FILTERS:
            params[SCOPE_FILTERS[self.connector_type]] = self.connector_ids

        try:
            page = 0
            while True:
                data = self._get(f"team/{self.team_id}/task", {**params, "page": page})
                tasks = data.get("tasks") or []
                for task in tasks:
                    task_id = str(task.get("id") or "")
                    if not task_id or task_id in seen:
                        continue
                    seen.add(task_id)
                    yield IngestionItem(
                        id=f"clickup:task:{task_id}",
                        source_ref=task,
                        last_modified=self._ms_to_datetime(task.get("date_updated")),
                    )
                if data.get("last_page") is True or len(tasks) < TASKS_PAGE_SIZE:
                    break
                page += 1

            logger.info(f"[{self.source_name}] Found {len(seen)} task(s)")
        finally:
            self._session.close()

    def get_raw_content(self, item: IngestionItem) -> str:
        """Build Markdown content from a ClickUp task."""
        task = item.source_ref
        parts: list[str] = [f"# {task.get('name') or task.get('id')}"]

        description = (task.get("markdown_description") or task.get("description") or "").strip()
        if description:
            parts.append(description)

        details = self._format_details(task)
        if details:
            parts.append(details)

        if self.retrieve_task_comments:
            comments = self._get_comments(item)
            if comments:
                lines = ["## Comments"]
                for comment in comments:
                    author = (comment.get("user") or {}).get("username") or "Unknown"
                    date = self._format_date(comment.get("date")) or ""
                    lines.append(f"**{author}** ({date}):\n{comment.get('comment_text') or ''}")
                parts.append("\n\n".join(lines))

        return "\n\n".join(parts)

    def get_item_name(self, item: IngestionItem) -> str:
        return slugify(f"clickup_task_{item.source_ref.get('id')}")

    def get_item_checksum(self, item: IngestionItem) -> str | None:
        """Return a checksum from the task ID and update time, without any extra request.

        ClickUp changes ``date_updated`` of a task when a top-level comment is added, so new comments
        re-ingest the task. Edits, deletions and replies of existing comments do not change it and are
        picked up when the task changes for another reason. Comments are part of the content, so the
        checksum also changes when ``retrieve_task_comments`` is switched.
        """
        task = item.source_ref
        suffix = ":comments" if self.retrieve_task_comments else ""
        return f"{task.get('id')}:{task.get('date_updated')}{suffix}"

    def get_extra_metadata(self, item: IngestionItem, _content: str, _metadata: dict[str, Any]) -> dict[str, Any]:
        task = item.source_ref
        priority = task.get("priority") or {}
        metadata: dict[str, Any] = {
            "task_id": task.get("id"),
            "task_name": task.get("name"),
            "url": task.get("url"),
            "status": (task.get("status") or {}).get("status"),
            "list": (task.get("list") or {}).get("name"),
            "project": (task.get("project") or {}).get("name"),
            "folder": (task.get("folder") or {}).get("name"),
            "space_id": (task.get("space") or {}).get("id"),
            "tags": [tag.get("name") for tag in task.get("tags") or [] if tag.get("name")],
            "priority": priority.get("priority") or "",
            "creator": self._format_user(task.get("creator")),
            "assignees": [self._format_user(user) for user in task.get("assignees") or []],
        }
        for field in DATE_FIELDS:
            value = self._format_date(task.get(field))
            if value:
                metadata[field] = value
        return {key: value for key, value in metadata.items() if value is not None}

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """GET an API v2 endpoint with client-side throttling and bounded retries."""
        self._throttle()
        resp = self._session.get(f"{CLICKUP_API_BASE_URL}/{endpoint}", params=params, headers=self._headers)
        resp.raise_for_status()
        return resp.json()

    def _throttle(self) -> None:
        """Block until a request fits in the per-minute limit."""
        now = time.monotonic()
        while self._request_times and now - self._request_times[0] >= 60:
            self._request_times.popleft()
        if len(self._request_times) >= self.max_requests_per_minute:
            wait = 60 - (now - self._request_times[0])
            logger.info(f"[{self.source_name}] Rate limit reached, sleeping {wait:.1f}s")
            time.sleep(max(wait, 0))
            now = time.monotonic()
            self._request_times.popleft()
        self._request_times.append(now)

    def _get_comments(self, item: IngestionItem) -> list[dict[str, Any]]:
        """Return all top-level comments of a task, newest first."""
        task_id = item.source_ref.get("id")
        comments: list[dict[str, Any]] = []
        seen: set[str] = set()
        params: dict[str, Any] = {}
        while True:
            page = self._get(f"task/{task_id}/comment", params).get("comments") or []
            new = [c for c in page if str(c.get("id")) not in seen]
            seen.update(str(c.get("id")) for c in new)
            comments.extend(new)
            if len(page) < COMMENTS_PAGE_SIZE or not new:
                break
            params = {"start": page[-1].get("date"), "start_id": page[-1].get("id")}

        return comments

    @staticmethod
    def _ms_to_datetime(value: Any) -> datetime | None:
        try:
            return datetime.fromtimestamp(int(value) / 1000, tz=UTC)
        except (ValueError, TypeError, OverflowError, OSError):
            return None

    @staticmethod
    def _format_date(value: Any) -> str | None:
        parsed = ClickUpIngestionJob._ms_to_datetime(value)
        return parsed.isoformat() if parsed else None

    @staticmethod
    def _format_user(user: dict[str, Any] | None) -> str:
        user = user or {}
        name = user.get("username") or ""
        email = user.get("email") or ""
        return f"{name} <{email}>" if name and email else name or email

    @staticmethod
    def _format_details(task: dict[str, Any]) -> str:
        priority = (task.get("priority") or {}).get("priority")
        rows = [
            ("Status", (task.get("status") or {}).get("status")),
            ("Priority", priority),
            ("List", (task.get("list") or {}).get("name")),
            ("Folder", (task.get("folder") or {}).get("name")),
            ("Tags", ", ".join(t.get("name") for t in task.get("tags") or [] if t.get("name"))),
            ("Creator", ClickUpIngestionJob._format_user(task.get("creator"))),
            ("Assignees", ", ".join(ClickUpIngestionJob._format_user(u) for u in task.get("assignees") or [])),
            ("Due", ClickUpIngestionJob._format_date(task.get("due_date"))),
        ]
        return "\n".join(f"**{label}:** {value}" for label, value in rows if value)
