# Standard library imports
import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

# Third-party imports
import gitlab
from llama_index.readers.gitlab import GitLabIssuesReader, GitLabRepositoryReader

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.parse import parse_bool, parse_list, parse_timestamp
from utils.text import slugify

logger = logging.getLogger(__name__)


class GitLabIngestionJob(IngestionJob):
    """Ingestion connector for GitLab repositories and issues.

    Uses LlamaIndex GitLabRepositoryReader and GitLabIssuesReader for all
    discovery and content fetching. No reader logic is duplicated here.

    Configuration (config.yaml):
        - config.gitlab_url: GitLab server URL (required, e.g. "https://gitlab.com")
        - config.personal_token: GitLab personal access token (required)
        - config.project_id: GitLab project ID, integer. Mutually exclusive with
          group_id — one of the two is required. Ingests repository files (and,
          if include_issues is set, that project's issues only).
        - config.group_id: GitLab group ID, integer. Mutually exclusive with
          project_id — one of the two is required. Ingests issues across every
          project in the group (include_issues must be set; repository files
          are not supported in group mode).
        - config.ref: Branch or commit ref for repository files (optional, default "main")
        - config.path: Sub-directory path to limit repository file loading (optional)
        - config.file_path: Single file path to load, instead of a directory (optional)
        - config.recursive: Whether to recurse into sub-directories (optional, default True)
        - config.files_iterator: Use iterator pagination for repository files to fetch all pages (optional, default True)
        - config.include_issues: Whether to ingest issues (optional, default False)
        - config.issues_state: Issue state filter "opened"/"closed"/"all" (optional, default "opened")
        - config.issues_labels: Comma-separated label filter (optional)
        - config.issues_assignee: Assignee username or ID filter (optional)
        - config.issues_author: Author username or ID filter (optional)
        - config.issues_milestone: Milestone title filter (optional)
        - config.issues_search: Free-text search filter (optional)
        - config.issues_get_all: Fetch all pages of issues (optional, default True;
          set to False to cap at the first page, GitLab's default page size of 20)
        - config.issues_confidential: Filter by confidential flag (optional)
        - config.issues_created_after: Only issues created after this ISO-8601 timestamp (optional)
        - config.issues_created_before: Only issues created before this ISO-8601 timestamp (optional)
        - config.issues_updated_after: Only issues updated after this ISO-8601 timestamp (optional)
        - config.issues_updated_before: Only issues updated before this ISO-8601 timestamp (optional)
        - config.issues_iids: Filter by specific issue IIDs (optional); a YAML list
          of integers or a comma-separated string ("1,2,3"), like issues_labels
        - config.issues_type: Issue type filter "issue"/"incident"/"test_case"/"task" (optional)
        - config.issues_non_archived: Exclude issues from archived projects (optional)
        - config.issues_scope: Scope filter "created_by_me"/"assigned_to_me"/"all" (optional)
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

        if self.project_id and self.group_id:
            raise ValueError(
                "project_id and group_id are mutually exclusive in GitLab connector config: "
                "repository files only ever come from project_id, while issues would silently "
                "switch to all group issues (overwriting project-scoped results) if group_id is "
                "also set. Configure one connector per scope instead."
            )

        # Repository options
        self.ref: str = str(cfg.get("ref", "main"))
        self.path: str | None = cfg.get("path") or None
        self.file_path: str | None = cfg.get("file_path") or None
        self.recursive: bool = parse_bool(cfg.get("recursive"), default=True)
        self.files_iterator: bool = parse_bool(cfg.get("files_iterator"), default=True)

        # Issue options
        self.include_issues: bool = parse_bool(cfg.get("include_issues"), default=False)
        self.issues_state: str = cfg.get("issues_state", "opened")
        self.issues_labels: list[str] | None = parse_list(cfg.get("issues_labels")) or None
        self.issues_assignee: str | None = cfg.get("issues_assignee") or None
        self.issues_author: str | None = cfg.get("issues_author") or None
        self.issues_milestone: str | None = cfg.get("issues_milestone") or None
        self.issues_search: str | None = cfg.get("issues_search") or None
        self.issues_get_all: bool = parse_bool(cfg.get("issues_get_all"), default=True)
        self.issues_confidential: bool | None = self._parse_bool_optional(cfg.get("issues_confidential"))
        self.issues_created_after: datetime | None = parse_timestamp(cfg.get("issues_created_after"))
        self.issues_created_before: datetime | None = parse_timestamp(cfg.get("issues_created_before"))
        self.issues_updated_after: datetime | None = parse_timestamp(cfg.get("issues_updated_after"))
        self.issues_updated_before: datetime | None = parse_timestamp(cfg.get("issues_updated_before"))
        self.issues_iids: list[int] | None = self._parse_int_list(cfg.get("issues_iids"))
        self.issues_type: GitLabIssuesReader.IssueType | None = self._resolve_issue_type_enum(cfg.get("issues_type"))
        self.issues_non_archived: bool | None = self._parse_bool_optional(cfg.get("issues_non_archived"))
        self.issues_scope: GitLabIssuesReader.Scope | None = self._resolve_scope_enum(cfg.get("issues_scope"))

        gl = gitlab.Gitlab(self.gitlab_url, private_token=self.personal_token)

        self._repo_reader: GitLabRepositoryReader | None = None
        self._issues_reader: GitLabIssuesReader | None = None
        # GitLabRepositoryReader's Document.extra_info never exposes the project's
        # web_url/path_with_namespace, only the numeric project_id (not a valid
        # browse-URL path segment) — fetch it once here so get_extra_metadata() can
        # build a real browse URL instead of the reader's broken raw API link.
        self._project_web_url: str | None = None

        if self.project_id:
            self._repo_reader = GitLabRepositoryReader(
                gitlab_client=gl,
                project_id=self.project_id,
            )
            self._project_web_url = gl.projects.get(self.project_id).web_url

        if self.include_issues:
            # GitLabIssuesReader.load_data() requires project_id or group_id at
            # call time, even though both are optional in its constructor
            # signature. The check above (line 67-68) already guarantees at
            # least one of self.project_id / self.group_id is set here, so
            # this reader is never constructed with both None.
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
                    iterator=self.files_iterator,
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
                scope = self.project_id or self.group_id
                for doc in docs:
                    yield IngestionItem(
                        id=f"gitlab:{scope}:issue:{self._issue_identity(doc)}",
                        source_ref=doc,
                        last_modified=parse_timestamp(
                            doc.metadata.get("created_at")  # GitLabIssuesReader does not expose updated_at
                        ),
                    )
            except Exception:
                logger.exception("[%s] Failed to load issues", self.source_name)
                raise

    def _issue_identity(self, doc: Any) -> str:
        """Return a stable, unique identifier for a GitLab issue document.

        doc.doc_id is the GitLab iid, which is project-scoped: two projects in
        the same group can share an iid, so it collides when only group_id is
        configured. The reader does not expose GitLab's instance-global issue
        id in metadata, but it does expose the API self-link ("url"), which is
        unique per project+issue (e.g. ".../projects/<project_id>/issues/<id>")
        and is safe to use as the identity key in both project- and
        group-scoped ingestion. Used by both list_items() and get_item_name()
        so item.id and item_name stay consistent for version tracking.

        Raises if "url" is missing or empty rather than falling back to
        doc.doc_id (the iid): that fallback would silently reintroduce the
        group-scoped collision this method exists to prevent, so a missing
        url must fail the item instead of passing through quietly.
        """
        issue_url = (doc.metadata or {}).get("url")
        if not issue_url:
            raise ValueError(
                f"[{self.source_name}] GitLab issue {doc.doc_id!r} has no 'url' metadata; "
                "cannot build a stable identity without it (project-scoped iid alone can "
                "collide across projects in group-scoped ingestion)"
            )
        return issue_url

    def get_raw_content(self, item: IngestionItem) -> str:
        doc = item.source_ref
        return doc.text or ""

    def get_item_name(self, item: IngestionItem) -> str:
        doc = item.source_ref
        extra = doc.metadata or {}

        if ":issue:" in item.id:
            scope = self.project_id or self.group_id
            name = f"gitlab_issue_{scope}_{slugify(self._issue_identity(doc))}"
        else:
            file_path = extra.get("file_path", doc.doc_id or "")
            name = slugify(file_path) if file_path else ""

        return name[:255] if name else item.id[:255]

    def get_extra_metadata(self, item: IngestionItem, _content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        doc = item.source_ref
        extra = doc.metadata or {}
        item_name = metadata.get("key", "")

        result: dict[str, Any] = {"gitlab_url": self.gitlab_url}

        if ":issue:" in item.id:
            # The reader embeds "{title}\n{description}" as doc.text and does not
            # expose the title in metadata separately; extract it here so citations
            # and filters get a human-readable name, same pattern as Jira's "title".
            title = (doc.text or "").split("\n", 1)[0]
            result.update(
                {
                    "item_type": "issue",
                    "title": title,
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
            file_path = extra.get("file_path", "")
            result.update(
                {
                    "item_type": "file",
                    "file_path": file_path,
                    # "file_name" is reserved by BaseMetadataSchema; process_item()
                    # overwrites it from get_item_name() and silently drops this
                    # extra, so the real basename never lands in the vector. Use a
                    # non-reserved key instead, same fix as OneDrive's
                    # "onedrive_file_name".
                    "gitlab_file_name": extra.get("file_name", item_name),
                    "url": self._file_browse_url(file_path),
                }
            )

        return result

    def _file_browse_url(self, file_path: str) -> str:
        """Build a browsable GitLab blob URL for a repository file.

        GitLabRepositoryReader's Document.extra_info only ever sets a broken raw
        API path (extra "/projects" segment, no ref, path unencoded) as "url" —
        not useful as citation metadata. Build a real "-/blob/<ref>/<path>" URL
        against the project's web_url instead, same idea as the Jira connector's
        issue_url/source fields. Falls back to the raw project_id-based URL if the
        project's web_url could not be resolved (should not normally happen, since
        it's fetched once in __init__ whenever project_id is set).
        """
        if not file_path:
            return ""
        base = self._project_web_url or f"{self.gitlab_url}/{self.project_id}"
        # Percent-encode "/" in the ref (safe="") so a ref like "feature/x" can't
        # be misread as extra path segments against the file path that follows;
        # the file path itself keeps "/" unescaped since those are real separators.
        encoded_ref = quote(self.ref, safe="")
        encoded_path = quote(file_path)
        return f"{base}/-/blob/{encoded_ref}/{encoded_path}"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_bool_optional(value: Any) -> bool | None:
        """Parse a tristate bool config value; returns None when unset."""
        if value is None:
            return None
        return parse_bool(value)

    @staticmethod
    def _parse_int_list(value: Any) -> list[int] | None:
        """Parse a YAML list or comma-separated string of issue IIDs into ints.

        Mirrors issues_labels' use of parse_list() so a comma-separated/
        env-substituted string ("1,2,3") works the same as a native YAML list.
        """
        items = parse_list(value)
        return [int(v) for v in items] or None

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
