# Standard library imports
import logging
import re
from collections.abc import Iterator
from typing import Any

# Third-party imports
from llama_index.core.async_utils import asyncio_run
from llama_index.readers.github import (
    GitHubAppAuth,
    GithubClient,
    GitHubIssuesClient,
    GitHubRepositoryIssuesReader,
    GithubRepositoryReader,
)

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class GitHubIngestionJob(IngestionJob):
    """Ingestion connector for GitHub repositories.

    Uses the LlamaIndex GithubRepositoryReader for all file discovery, content
    fetching, branch/commit resolution, and filtering.  For issues it uses
    GitHubRepositoryIssuesReader.  This connector only adds ROAT-specific
    orchestration: config parsing, validation, and IngestionItem production.

    Configuration (config.yaml):
        Auth — exactly one of:
            - config.personal_token: GitHub PAT (mutually exclusive with GitHub App auth)
            - config.github_app_id + config.github_app_installation_id +
              config.github_app_private_key: GitHub App credentials

        Repository:
            - config.owner: Repository owner / org (required)
            - config.repo: Repository name (required)
            - config.branch: Branch name (mutually exclusive with commit_sha)
            - config.commit_sha: Commit SHA (mutually exclusive with branch)
              If neither is set, branch defaults to "main".

        File filters — mutually exclusive pairs:
            - config.include_extensions: comma-separated extensions, e.g. "md,py"
            - config.exclude_extensions: comma-separated extensions (mutually exclusive with include_extensions)
            - config.include_directories: comma-separated directories (mutually exclusive with exclude_directories)
            - config.exclude_directories: comma-separated directories (mutually exclusive with include_directories)

        Issues:
            - config.include_issues: bool (default false)
            - config.include_issues_labels: comma-separated labels to include
              (mutually exclusive with exclude_issues_labels)
            - config.exclude_issues_labels: comma-separated labels to exclude
              (mutually exclusive with include_issues_labels)

        - config.concurrent_requests: number of concurrent API requests (default 5)

    Constraints:
        - personal_token and GitHub App credentials are mutually exclusive
        - branch and commit_sha are mutually exclusive
        - include_extensions and exclude_extensions are mutually exclusive
        - include_directories and exclude_directories are mutually exclusive
        - include_issues_labels and exclude_issues_labels are mutually exclusive
    """

    @property
    def source_type(self) -> str:
        return "github"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        # ------------------------------------------------------------------
        # Auth validation
        # ------------------------------------------------------------------
        personal_token = cfg.get("personal_token", "").strip()
        app_id = cfg.get("github_app_id", "").strip()
        app_installation_id = cfg.get("github_app_installation_id", "").strip()
        app_private_key = cfg.get("github_app_private_key", "").strip()

        has_pat = bool(personal_token)
        has_app = bool(app_id or app_installation_id or app_private_key)

        if has_pat and has_app:
            raise ValueError(
                "personal_token and GitHub App credentials are mutually exclusive in GitHub connector config"
            )
        if has_app and not (app_id and app_installation_id and app_private_key):
            raise ValueError(
                "github_app_id, github_app_installation_id, and github_app_private_key "
                "are all required for GitHub App authentication"
            )
        if not has_pat and not has_app:
            raise ValueError(
                "Either personal_token or GitHub App credentials "
                "(github_app_id, github_app_installation_id, github_app_private_key) "
                "are required in GitHub connector config"
            )

        # ------------------------------------------------------------------
        # Repository params
        # ------------------------------------------------------------------
        self.owner = cfg.get("owner", "").strip()
        self.repo = cfg.get("repo", "").strip()
        if not self.owner:
            raise ValueError("owner is required in GitHub connector config")
        if not self.repo:
            raise ValueError("repo is required in GitHub connector config")

        branch = cfg.get("branch", "").strip()
        commit_sha = cfg.get("commit_sha", "").strip()
        if branch and commit_sha:
            raise ValueError("branch and commit_sha are mutually exclusive in GitHub connector config")
        # Default to "main" if neither is provided
        self.branch: str | None = branch or (None if commit_sha else "main")
        self.commit_sha: str | None = commit_sha or None

        # ------------------------------------------------------------------
        # File / directory filters validation (mutually exclusive pairs)
        # ------------------------------------------------------------------
        include_ext = self._parse_list(cfg.get("include_extensions", ""))
        exclude_ext = self._parse_list(cfg.get("exclude_extensions", ""))
        include_dirs = self._parse_list(cfg.get("include_directories", ""))
        exclude_dirs = self._parse_list(cfg.get("exclude_directories", ""))

        if include_ext and exclude_ext:
            raise ValueError(
                "include_extensions and exclude_extensions are mutually exclusive in GitHub connector config"
            )
        if include_dirs and exclude_dirs:
            raise ValueError(
                "include_directories and exclude_directories are mutually exclusive in GitHub connector config"
            )

        # ------------------------------------------------------------------
        # Issues params
        # ------------------------------------------------------------------
        self.include_issues: bool = bool(cfg.get("include_issues", False))

        include_labels = self._parse_list(cfg.get("include_issues_labels", ""))
        exclude_labels = self._parse_list(cfg.get("exclude_issues_labels", ""))

        if include_labels and exclude_labels:
            raise ValueError(
                "include_issues_labels and exclude_issues_labels are mutually exclusive in GitHub connector config"
            )

        self._include_labels = include_labels
        self._exclude_labels = exclude_labels

        # ------------------------------------------------------------------
        # Build readers
        # ------------------------------------------------------------------
        if has_app:
            app_auth = GitHubAppAuth(
                app_id=app_id,
                private_key=app_private_key,
                installation_id=app_installation_id,
            )
            self._github_client = GithubClient(github_app_auth=app_auth)
            issues_client = GitHubIssuesClient(github_app_auth=app_auth)
        else:
            self._github_client = GithubClient(github_token=personal_token)
            issues_client = GitHubIssuesClient(github_token=personal_token)

        filter_dirs: tuple | None = None
        if include_dirs:
            filter_dirs = (
                include_dirs,
                GithubRepositoryReader.FilterType.INCLUDE,
            )
        elif exclude_dirs:
            filter_dirs = (
                exclude_dirs,
                GithubRepositoryReader.FilterType.EXCLUDE,
            )

        filter_exts: tuple | None = None
        if include_ext:
            filter_exts = (
                include_ext,
                GithubRepositoryReader.FilterType.INCLUDE,
            )
        elif exclude_ext:
            filter_exts = (
                exclude_ext,
                GithubRepositoryReader.FilterType.EXCLUDE,
            )

        self.concurrent_requests: int = int(cfg.get("concurrent_requests", 5))

        self._repo_reader = GithubRepositoryReader(
            github_client=self._github_client,
            owner=self.owner,
            repo=self.repo,
            filter_directories=filter_dirs,
            filter_file_extensions=filter_exts,
            verbose=False,
            concurrent_requests=self.concurrent_requests,
        )

        self._issues_reader = GitHubRepositoryIssuesReader(
            github_client=issues_client,
            owner=self.owner,
            repo=self.repo,
        )

        logger.info(
            f"Initialized GitHub connector "
            f"(owner={self.owner!r}, repo={self.repo!r}, "
            f"branch={self.branch!r}, commit_sha={self.commit_sha!r}, "
            f"include_issues={self.include_issues}, "
            f"filter_dirs={filter_dirs}, filter_exts={filter_exts})"
        )

    # ------------------------------------------------------------------
    # IngestionJob abstract method implementations
    # ------------------------------------------------------------------

    def list_items(self) -> Iterator[IngestionItem]:
        """Yield one IngestionItem per repository file, and optionally per issue."""
        logger.info(f"[{self.source_name}] Discovering GitHub files")
        try:
            if self.branch:
                docs = self._repo_reader.load_data(branch=self.branch)
            else:
                docs = self._repo_reader.load_data(commit_sha=self.commit_sha)
        except Exception as e:
            logger.error(f"[{self.source_name}] Failed to load repository files: {e}")
            docs = []

        file_last_modified = self._get_head_commit_date()

        for doc in docs:
            file_path = doc.metadata.get("file_path", doc.doc_id or "unknown")
            yield IngestionItem(
                id=f"github:{self.owner}/{self.repo}:{file_path}",
                source_ref=doc,
                last_modified=file_last_modified,
            )

        if self.include_issues:
            logger.info(f"[{self.source_name}] Discovering GitHub issues")
            label_filters = self._build_label_filters()
            try:
                issues = self._issues_reader.load_data(
                    state=GitHubRepositoryIssuesReader.IssueState.ALL,
                    labelFilters=label_filters if label_filters else None,
                )
            except Exception as e:
                logger.error(f"[{self.source_name}] Failed to load issues: {e}")
                issues = []

            for doc in issues:
                # The GitHub Issues API returns both issues and PRs; skip PRs
                if "/pull/" in doc.metadata.get("source", ""):
                    continue
                last_modified = doc.metadata.get("created_at")
                yield IngestionItem(
                    id=f"github:{self.owner}/{self.repo}:issue:{doc.doc_id}",
                    source_ref=doc,
                    last_modified=last_modified,
                )

    def get_raw_content(self, item: IngestionItem) -> str:
        """Return the already-fetched document text.

        The LlamaIndex readers load full content during list_items(), so
        no additional API call is needed here.
        """
        doc = item.source_ref
        return doc.text or ""

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe identifier for the item."""
        doc = item.source_ref
        path = doc.metadata.get("file_path") or doc.doc_id or item.id
        safe = re.sub(r"[^\w\-/\.]", "_", str(path))
        return safe[:255]

    def get_document_metadata(
        self,
        item: IngestionItem,
        item_name: str,
        checksum: str,
        version: int,
        last_modified: Any,
    ) -> dict[str, Any]:
        """Build metadata dict with GitHub-specific fields."""
        doc = item.source_ref
        doc_metadata = doc.metadata or {}

        metadata = super().get_document_metadata(item, item_name, checksum, version, last_modified)

        is_issue = ":issue:" in item.id
        if is_issue:
            url = doc_metadata.get("source", doc_metadata.get("url", ""))
            issue_meta: dict[str, Any] = {
                "owner": self.owner,
                "repo": self.repo,
                "item_type": "issue",
                "issue_number": doc.doc_id,
                "url": url,
            }
            for field in ("state", "labels", "assignee", "closed_at"):
                if doc_metadata.get(field) is not None:
                    issue_meta[field] = doc_metadata[field]
            metadata.update(issue_meta)
        else:
            file_path = doc_metadata.get("file_path", "")
            file_name = doc_metadata.get("file_name", "")
            url = doc_metadata.get("url", "")
            metadata.update(
                {
                    "owner": self.owner,
                    "repo": self.repo,
                    "item_type": "file",
                    "file_path": file_path,
                    "file_name": file_name,
                    "url": url,
                }
            )
        return metadata

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _get_head_commit_date(self) -> str | None:
        """Return the committer date of the branch HEAD commit.

        Uses two API calls per ingestion run: getBranch to fetch the HEAD
        commit SHA, then getCommit to extract its committer date.
        Falls back to None if the branch is not set or either call fails.
        """
        if not self.branch:
            return None
        try:
            branch_response = asyncio_run(
                self._github_client.request(
                    "getBranch",
                    "GET",
                    owner=self.owner,
                    repo=self.repo,
                    branch=self.branch,
                )
            )
            commit_sha = branch_response.json()["commit"]["sha"]
            commit_response = asyncio_run(
                self._github_client.request(
                    "getCommit",
                    "GET",
                    owner=self.owner,
                    repo=self.repo,
                    commit_sha=commit_sha,
                )
            )
            return commit_response.json()["commit"]["committer"]["date"]
        except Exception as e:
            logger.warning(f"[{self.source_name}] Could not fetch HEAD commit date: {e}")
            return None

    def _build_label_filters(self):
        """Build label filter list for GitHubRepositoryIssuesReader."""
        if self._include_labels:
            return [(label, GitHubRepositoryIssuesReader.FilterType.INCLUDE) for label in self._include_labels]
        if self._exclude_labels:
            return [(label, GitHubRepositoryIssuesReader.FilterType.EXCLUDE) for label in self._exclude_labels]
        return None

    @staticmethod
    def _parse_list(value: Any) -> list[str]:
        """Parse a comma-separated string or list into a list of stripped strings."""
        if not value:
            return []
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        return [v.strip() for v in str(value).split(",") if v.strip()]
