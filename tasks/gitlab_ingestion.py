# Standard library imports
import logging
import re
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

# Third-party imports
import gitlab
from llama_index.readers.gitlab import GitLabIssuesReader, GitLabRepositoryReader

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)


class GitLabIngestionJob(IngestionJob):
    """Ingestion connector for GitLab repositories and issues.

    Uses LlamaIndex GitLabRepositoryReader and GitLabIssuesReader for all
    discovery and content fetching. No reader logic is duplicated here.

    Configuration (config.yaml):
        - config.gitlab_url: GitLab server URL (required, e.g. "https://gitlab.com")
        - config.personal_token: GitLab personal access token (required)
        - config.project_id: GitLab project ID, integer (required unless group_id set)
        - config.group_id: GitLab group ID for issues (optional, mutually exclusive with project_id for repo)
        - config.ref: Branch or commit ref for repository files (optional, default "main")
        - config.path: Sub-directory path to limit repository file loading (optional)
        - config.recursive: Whether to recurse into sub-directories (optional, default True)
        - config.include_issues: Whether to ingest issues (optional, default False)
        - config.issues_state: Issue state filter "opened"/"closed"/"all" (optional, default "opened")
        - config.issues_labels: Comma-separated label filter (optional)
        - config.issues_assignee: Assignee username or ID filter (optional)
        - config.issues_author: Author username or ID filter (optional)
        - config.issues_milestone: Milestone title filter (optional)
        - config.issues_search: Free-text search filter (optional)
        - config.issues_get_all: Fetch all pages of issues (optional, default False)
    """

    @property
    def source_type(self) -> str:
        return "gitlab"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        # Auth
        self.gitlab_url = cfg.get("gitlab_url", "").rstrip("/")
        if not self.gitlab_url:
            raise ValueError("gitlab_url is required in GitLab connector config")

        self.personal_token = cfg.get("personal_token", "").strip()
        if not self.personal_token:
            raise ValueError("personal_token is required in GitLab connector config")

        # Project / group
        self.project_id: int | None = cfg.get("project_id")
        self.group_id: int | None = cfg.get("group_id")

        if not self.project_id and not self.group_id:
            raise ValueError("At least one of project_id or group_id is required in GitLab connector config")

        # Repository options
        self.ref: str = str(cfg.get("ref", "main"))
        self.path: str | None = cfg.get("path") or None
        self.file_path: str | None = cfg.get("file_path") or None
        self.recursive: bool = self._parse_bool(cfg.get("recursive"), default=True)

        # Issue options
        self.include_issues: bool = self._parse_bool(cfg.get("include_issues"), default=False)
        self.issues_state: str = cfg.get("issues_state", "opened")
        self.issues_labels: list[str] | None = self._parse_list(cfg.get("issues_labels"))
        self.issues_assignee: str | None = cfg.get("issues_assignee") or None
        self.issues_author: str | None = cfg.get("issues_author") or None
        self.issues_milestone: str | None = cfg.get("issues_milestone") or None
        self.issues_search: str | None = cfg.get("issues_search") or None
        self.issues_get_all: bool = self._parse_bool(cfg.get("issues_get_all"), default=False)
        self.issues_confidential: bool | None = self._parse_bool_optional(cfg.get("issues_confidential"))
        self.issues_created_after: datetime | None = self._parse_timestamp(cfg.get("issues_created_after"))
        self.issues_created_before: datetime | None = self._parse_timestamp(cfg.get("issues_created_before"))
        self.issues_updated_after: datetime | None = self._parse_timestamp(cfg.get("issues_updated_after"))
        self.issues_updated_before: datetime | None = self._parse_timestamp(cfg.get("issues_updated_before"))
        self.issues_iids: list[int] | None = cfg.get("issues_iids") or None
        self.issues_type: GitLabIssuesReader.IssueType | None = self._resolve_issue_type_enum(cfg.get("issues_type"))
        self.issues_non_archived: bool | None = self._parse_bool_optional(cfg.get("issues_non_archived"))
        self.issues_scope: GitLabIssuesReader.Scope | None = self._resolve_scope_enum(cfg.get("issues_scope"))

        gl = gitlab.Gitlab(self.gitlab_url, private_token=self.personal_token)

        self._repo_reader: GitLabRepositoryReader | None = None
        self._issues_reader: GitLabIssuesReader | None = None

        if self.project_id:
            self._repo_reader = GitLabRepositoryReader(
                gitlab_client=gl,
                project_id=self.project_id,
            )

        if self.include_issues:
            self._issues_reader = GitLabIssuesReader(
                gitlab_client=gl,
                project_id=self.project_id if self.project_id else None,
                group_id=self.group_id if self.group_id else None,
            )

        if self._repo_reader is None and self._issues_reader is None:
            raise ValueError(
                "Invalid GitLab connector config: no ingestion target enabled. "
                "Set project_id for repository ingestion or enable include_issues for group/project issues."
            )

        logger.info(
            f"Initialized GitLab connector (url={self.gitlab_url!r}, "
            f"project_id={self.project_id}, group_id={self.group_id}, "
            f"ref={self.ref!r}, include_issues={self.include_issues})"
        )

    # ------------------------------------------------------------------
    # IngestionJob abstract method implementations
    # ------------------------------------------------------------------

    def list_items(self) -> Iterator[IngestionItem]:
        """Yield IngestionItems for repository files and optionally issues."""

        # Repository files
        if self._repo_reader is not None:
            logger.info(f"[{self.source_name}] Discovering GitLab repository files")
            try:
                docs = self._repo_reader.load_data(
                    ref=self.ref,
                    file_path=self.file_path,
                    path=self.path,
                    recursive=self.recursive,
                )
                for doc in docs:
                    file_path = doc.metadata.get("file_path", doc.doc_id)
                    yield IngestionItem(
                        id=f"gitlab:{self.project_id}:{self.ref}:file:{file_path}",
                        source_ref=doc,
                        last_modified=datetime.now(
                            UTC
                        ),  # GitLab reader does not expose commit dates; use ingestion time
                    )
            except Exception:
                logger.exception("[%s] Failed to load repository files", self.source_name)
                raise

        # Issues
        if self.include_issues and self._issues_reader is not None:
            logger.info(f"[{self.source_name}] Discovering GitLab issues")
            try:
                state_enum = self._resolve_state_enum(self.issues_state)
                docs = self._issues_reader.load_data(
                    state=state_enum,
                    labels=self.issues_labels or None,
                    assignee=self.issues_assignee,
                    author=self.issues_author,
                    milestone=self.issues_milestone,
                    search=self.issues_search,
                    get_all=self.issues_get_all,
                    confidential=self.issues_confidential,
                    created_after=self.issues_created_after,
                    created_before=self.issues_created_before,
                    updated_after=self.issues_updated_after,
                    updated_before=self.issues_updated_before,
                    iids=self.issues_iids,
                    issue_type=self.issues_type,
                    non_archived=self.issues_non_archived,
                    scope=self.issues_scope,
                )
                for doc in docs:
                    # Use global id (unique across instance) for group mode; iid is project-scoped only
                    issue_id = (
                        doc.metadata.get("id") or doc.doc_id if self.group_id and not self.project_id else doc.doc_id
                    )
                    yield IngestionItem(
                        id=f"gitlab:{self.project_id or self.group_id}:issue:{issue_id}",
                        source_ref=doc,
                        last_modified=self._parse_timestamp(
                            doc.metadata.get("created_at")  # GitLabIssuesReader does not expose updated_at
                        ),
                    )
            except Exception:
                logger.exception("[%s] Failed to load issues", self.source_name)
                raise

    def get_raw_content(self, item: IngestionItem) -> str:
        doc = item.source_ref
        return doc.text or ""

    def get_item_name(self, item: IngestionItem) -> str:
        doc = item.source_ref
        extra = doc.metadata or {}

        if ":issue:" in item.id:
            iid = doc.doc_id
            name = f"gitlab_issue_{self.project_id or self.group_id}_{iid}"
        else:
            file_path = extra.get("file_path", doc.doc_id or "")
            name = re.sub(r"[^\w\-_\.]", "_", file_path)

        return name[:255] if name else item.id[:255]

    def get_extra_metadata(self, item: IngestionItem, _content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        doc = item.source_ref
        extra = doc.metadata or {}
        item_name = metadata.get("key", "")

        result: dict[str, Any] = {"gitlab_url": self.gitlab_url}

        if ":issue:" in item.id:
            result.update(
                {
                    "item_type": "issue",
                    "issue_number": doc.doc_id,
                    "state": extra.get("state", ""),
                    "labels": extra.get("labels", []),
                    "url": extra.get("source", extra.get("url", "")),
                }
            )
            if extra.get("assignee"):
                result["assignee"] = extra["assignee"]
            if extra.get("author"):
                result["author"] = extra["author"]
            if extra.get("closed_at"):
                result["closed_at"] = extra["closed_at"]
        else:
            result.update(
                {
                    "item_type": "file",
                    "file_path": extra.get("file_path", ""),
                    "file_name": extra.get("file_name", item_name),
                    "url": extra.get("url", ""),
                }
            )

        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_bool(value: Any, default: bool = False) -> bool:
        """Parse a config value to bool, safely handling string inputs like 'false'."""
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    @staticmethod
    def _parse_bool_optional(value: Any) -> bool | None:
        """Parse a config value to bool, returning None if not set."""
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    @staticmethod
    def _parse_list(value: Any) -> list[str] | None:
        """Parse a comma-separated string or list into a list of strings."""
        if not value:
            return None
        if isinstance(value, list):
            return [s for v in value if (s := str(v).strip())] or None
        return [v.strip() for v in str(value).split(",") if v.strip()] or None

    @staticmethod
    def _resolve_enum(enum_class, value, default=None):
        """Resolve a config string to an enum member by value, with optional default."""
        if not value:
            return default
        try:
            return enum_class(str(value).lower())
        except ValueError:
            return default

    @classmethod
    def _resolve_state_enum(cls, state: str) -> GitLabIssuesReader.IssueState:
        return cls._resolve_enum(GitLabIssuesReader.IssueState, state, GitLabIssuesReader.IssueState.OPEN)

    @classmethod
    def _resolve_scope_enum(cls, scope: str | None) -> GitLabIssuesReader.Scope | None:
        return cls._resolve_enum(GitLabIssuesReader.Scope, scope)

    @classmethod
    def _resolve_issue_type_enum(cls, issue_type: str | None) -> GitLabIssuesReader.IssueType | None:
        return cls._resolve_enum(GitLabIssuesReader.IssueType, issue_type)

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime | None:
        """Parse an ISO-8601 timestamp string into a datetime, or return None."""
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None
