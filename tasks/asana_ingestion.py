import hashlib
import json
import logging
from collections.abc import Iterator
from typing import Any

import asana

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.parse import parse_bool, parse_timestamp
from utils.text import slugify

logger = logging.getLogger(__name__)

_TASK_FIELDS = ",".join(
    [
        "name",
        "notes",
        "completed",
        "permalink_url",
        "modified_at",
        "created_at",
        "due_on",
        "tags.name",
        "assignee.gid",
        "assignee.name",
        "followers.gid",
        "followers.name",
        "custom_fields.name",
        "custom_fields.display_value",
        "memberships.project.gid",
        "memberships.project.name",
    ]
)
_COMMENT_FIELDS = "text,created_at,created_by.name"


class AsanaIngestionJob(IngestionJob):
    """Ingestion connector for Asana tasks.

    Fetches tasks (title, notes, comments, custom fields, assignee and followers)
    from one Asana workspace or from a single project, and stores them in the
    vector store. Uses the official ``asana`` SDK for all API interactions.

    Workspace mode ingests tasks of every non-archived project the token can
    reach. Tasks that belong to no project are not returned by the Asana API
    for project listings and are not ingested. A task shared by several
    projects is ingested once.

    Configuration (config.yaml):
        - config.asana_token: Asana personal access token (required)
        - config.workspace_id: Asana workspace GID (required)
        - config.project_id: Asana project GID (optional; if the key is omitted, all
          projects of the workspace are ingested)
        - config.load_comments: Whether to include task comments (optional, default True)
        - config.max_comments: Maximum comments per task, newest first (optional, default 10)
    """

    @property
    def source_type(self) -> str:
        return "asana"

    def __init__(self, config: dict):
        super().__init__(config)
        cfg = config.get("config", {})

        self.asana_token = str(cfg.get("asana_token") or "").strip()
        if not self.asana_token:
            raise ValueError("asana_token is required in Asana connector config")

        self.workspace_id = self._parse_gid(cfg.get("workspace_id"), "workspace_id")
        # An omitted key means workspace mode. A key that is present but blank is a
        # configuration error, so an empty env var never widens a project to the workspace.
        self.project_id = self._parse_gid(cfg.get("project_id"), "project_id") if "project_id" in cfg else ""

        self.load_comments = parse_bool(cfg.get("load_comments"), default=True)
        self.max_comments = int(cfg.get("max_comments", 10))
        if self.max_comments <= 0:
            raise ValueError("max_comments must be positive")

        configuration = asana.Configuration()
        configuration.access_token = self.asana_token
        api_client = asana.ApiClient(configuration)
        self._tasks_api = asana.TasksApi(api_client)
        self._projects_api = asana.ProjectsApi(api_client)
        self._stories_api = asana.StoriesApi(api_client)

        logger.info(
            f"Initialized Asana connector (workspace_id={self.workspace_id}, project_id={self.project_id or 'all'}, "
            f"load_comments={self.load_comments}, max_comments={self.max_comments})"
        )

    def list_items(self) -> Iterator[IngestionItem]:
        """Yield one IngestionItem per Asana task. API errors propagate.

        Comments are fetched here, not in get_raw_content(): the base class
        swallows exceptions raised while processing an item, which would turn a
        failed comments request into a silently skipped task.
        """
        seen: set[str] = set()
        total = 0
        for project_gid in self._project_gids():
            for task in self._tasks_api.get_tasks_for_project(project_gid, {"opt_fields": _TASK_FIELDS}):
                gid = str(task.get("gid") or "").strip()
                if not gid:
                    raise ValueError(f"Asana returned a task without gid in project {project_gid}")
                if gid in seen:
                    continue
                seen.add(gid)
                total += 1
                yield IngestionItem(
                    id=f"asana:{self.source_name}:task:{gid}",
                    source_ref=(task, self._build_comments_section(gid)),
                    last_modified=parse_timestamp(task.get("modified_at")),
                )
        logger.info(f"[{self.source_name}] Found {total} task(s)")

    def get_raw_content(self, item: IngestionItem) -> str:
        """Build content from the task title, notes and comments."""
        task, comments = item.source_ref
        parts: list[str] = [f"# {task.get('name') or ''}".strip()]

        notes = (task.get("notes") or "").strip()
        if notes:
            parts.append(notes)

        if comments:
            parts.append(comments)

        return "\n\n".join(parts)

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a tracker key made of a hash of the source name and item ID, and the task GID.

        Same scheme as the GitLab connector: the NUL separator keeps the pair
        unambiguous, 128 bits make accidental collisions negligible, and the
        digest comes first. Two sources never share a key, and the key does not
        depend on how the source name sanitizes. Renaming a source changes every
        key, so its tasks are ingested again.
        """
        task, _comments = item.source_ref
        digest = hashlib.sha256(f"{self.source_name}\0{item.id}".encode()).hexdigest()[:32]
        return f"asana_{digest}_{slugify(str(task.get('gid')))}"

    def get_item_checksum(self, item: IngestionItem) -> str | None:
        """Hash the content together with the metadata.

        The base class computes metadata only after the unchanged check, so a
        change of assignee, followers, custom fields or completion state would
        otherwise never reach the stored metadata.
        """
        content = self.get_raw_content(item)
        metadata = json.dumps(self.get_extra_metadata(item, content, {}), sort_keys=True)
        return hashlib.md5(f"{content}\n{metadata}".encode(), usedforsecurity=False).hexdigest()

    def get_extra_metadata(self, item: IngestionItem, _content: str, _metadata: dict[str, Any]) -> dict[str, Any]:
        """Return Asana-specific metadata fields. Empty values are omitted."""
        task, _comments = item.source_ref
        assignee = (task.get("assignee") or {}).get("name")
        followers = [f.get("name") for f in (task.get("followers") or []) if f.get("name")]
        tags = [t.get("name") for t in (task.get("tags") or []) if t.get("name")]
        projects = [
            m["project"]["name"] for m in (task.get("memberships") or []) if (m.get("project") or {}).get("name")
        ]
        # A list keeps same-named project-local fields apart; a name-keyed dict would drop one.
        custom_fields = [
            f"{f['name']}: {f['display_value']}"
            for f in (task.get("custom_fields") or [])
            if f.get("name") and f.get("display_value") not in (None, "")
        ]
        extra: dict[str, Any] = {
            "task_id": str(task.get("gid")),
            "task_title": task.get("name") or "",
            "url": task.get("permalink_url") or "",
            "completed": bool(task.get("completed")),
            "created_at": task.get("created_at") or "",
            "due_on": task.get("due_on") or "",
            "tags": tags,
            "workspace_id": self.workspace_id,
            "assignee": assignee or "",
            "followers": followers,
            "projects": projects,
            "custom_fields": custom_fields,
        }
        return {k: v for k, v in extra.items() if v not in ("", [], {})}

    def _project_gids(self) -> Iterator[str]:
        """Yield the project GIDs to ingest for the configured mode."""
        if self.project_id:
            project = self._projects_api.get_project(self.project_id, {"opt_fields": "workspace.gid"})
            workspace_gid = str(((project or {}).get("workspace") or {}).get("gid") or "")
            if workspace_gid != self.workspace_id:
                raise ValueError(
                    f"Asana project {self.project_id} belongs to workspace {workspace_gid!r}, "
                    f"not to configured workspace {self.workspace_id}"
                )
            yield self.project_id
            return

        projects = self._projects_api.get_projects(
            {"workspace": self.workspace_id, "archived": False, "opt_fields": "gid"}
        )
        for project in projects:
            yield str(project["gid"])

    def _build_comments_section(self, task_gid: str) -> str:
        """Fetch and format the newest comments of a task. API errors propagate."""
        if not self.load_comments:
            return ""
        stories = self._stories_api.get_stories_for_task(
            task_gid,
            {"resource_subtype": "comment_added", "sort_ascending": False, "opt_fields": _COMMENT_FIELDS},
            item_limit=self.max_comments,
        )
        lines: list[str] = []
        for story in stories:
            text = (story.get("text") or "").strip()
            if not text:
                continue
            author = (story.get("created_by") or {}).get("name") or "Unknown"
            lines.append(f"**{author}** ({story.get('created_at') or ''}):\n{text}")
        if not lines:
            return ""
        return "## Comments\n\n" + "\n\n".join(lines)

    @staticmethod
    def _parse_gid(value: Any, key: str) -> str:
        gid = str(value).strip() if value is not None else ""
        if not gid:
            raise ValueError(f"{key} is required in Asana connector config")
        if not gid.isdigit():
            raise ValueError(f"{key} must be a numeric Asana GID, got {gid!r}")
        return gid
