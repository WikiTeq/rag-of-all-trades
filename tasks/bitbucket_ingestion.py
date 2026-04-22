# Standard library imports
import logging
import re
from collections.abc import Iterator
from datetime import datetime
from typing import Any

# Third-party imports
import requests
from requests.auth import HTTPBasicAuth

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class BitbucketClient:
    """Thin HTTP client for the Bitbucket REST API v2.0.

    Handles authentication, request execution, and cursor-based pagination.
    """

    API_BASE = "https://api.bitbucket.org/2.0"

    def __init__(self, username: str, api_token: str) -> None:
        self._auth = HTTPBasicAuth(username, api_token)

    def list_files(self, workspace: str, repo: str, branch: str, path: str = "") -> Iterator[dict[str, Any]]:
        """Yield all ``commit_file`` entries from the repository, recursively.

        Paginates each directory listing via the ``next`` cursor and recurses
        into ``commit_directory`` entries.
        """
        url = f"{self.API_BASE}/repositories/{workspace}/{repo}/src/{branch}/{path}"
        params: dict[str, Any] = {"pagelen": 100}

        while url:
            try:
                resp = requests.get(url, auth=self._auth, params=params, timeout=30)
                resp.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"Bitbucket API error listing {path or '/'!r} in {workspace}/{repo}@{branch}: {e}")
                break

            data = resp.json()
            params = {}

            for entry in data.get("values", []):
                entry_type = entry.get("type")
                if entry_type == "commit_file":
                    yield entry
                elif entry_type == "commit_directory":
                    dir_path = entry.get("path", "")
                    if dir_path:
                        yield from self.list_files(workspace, repo, branch, dir_path)

            url = data.get("next", "")

    def get_file_content(self, workspace: str, repo: str, branch: str, path: str) -> str:
        """Fetch and return the raw text content of a single file."""
        url = f"{self.API_BASE}/repositories/{workspace}/{repo}/src/{branch}/{path}"
        try:
            resp = requests.get(url, auth=self._auth, timeout=30)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            logger.error(f"Bitbucket API error fetching {path!r} from {workspace}/{repo}@{branch}: {e}")
            return ""


class BitbucketIngestionJob(IngestionJob):
    """Ingestion connector for Bitbucket Cloud repositories.

    Fetches repository files via the Bitbucket REST API v2.0, converts their
    content to text, and stores it in the vector store. Supports workspace/
    repository scoping, branch selection, recursive file walking with
    include/exclude filtering, and cursor-based pagination.

    Configuration (config.yaml):
        - config.username: Bitbucket username (required)
        - config.api_token: Bitbucket API token (required)
        - config.workspace: Workspace slug (required)
        - config.repo: Repository slug (required)
        - config.branch: Branch or ref to ingest (optional, default "master")
        - config.include_extensions: Comma-separated file extensions to include,
          e.g. "md,txt" (optional; mutually exclusive with exclude_extensions)
        - config.exclude_extensions: Comma-separated file extensions to exclude
          (optional; mutually exclusive with include_extensions)
        - config.include_directories: Comma-separated directory prefixes to
          include, e.g. "docs,src" (optional; mutually exclusive with
          exclude_directories)
        - config.exclude_directories: Comma-separated directory prefixes to
          exclude (optional; mutually exclusive with include_directories)
    """

    @property
    def source_type(self) -> str:
        return "bitbucket"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        self.username = cfg.get("username", "").strip()
        if not self.username:
            raise ValueError("username is required in Bitbucket connector config")

        self.api_token = cfg.get("api_token", "").strip()
        if not self.api_token:
            raise ValueError("api_token is required in Bitbucket connector config")

        self.workspace = cfg.get("workspace", "").strip()
        if not self.workspace:
            raise ValueError("workspace is required in Bitbucket connector config")

        self.repo = cfg.get("repo", "").strip()
        if not self.repo:
            raise ValueError("repo is required in Bitbucket connector config")

        self.branch = cfg.get("branch", "master").strip() or "master"

        include_ext = cfg.get("include_extensions", "")
        exclude_ext = cfg.get("exclude_extensions", "")
        if include_ext and exclude_ext:
            raise ValueError(
                "include_extensions and exclude_extensions are mutually exclusive in Bitbucket connector config"
            )

        include_dir = cfg.get("include_directories", "")
        exclude_dir = cfg.get("exclude_directories", "")
        if include_dir and exclude_dir:
            raise ValueError(
                "include_directories and exclude_directories are mutually exclusive in Bitbucket connector config"
            )

        self.include_extensions: set[str] = self._parse_csv(include_ext)
        self.exclude_extensions: set[str] = self._parse_csv(exclude_ext)
        self.include_directories: set[str] = self._parse_csv(include_dir)
        self.exclude_directories: set[str] = self._parse_csv(exclude_dir)

        self._client = BitbucketClient(self.username, self.api_token)

        logger.info(
            f"Initialized Bitbucket connector for {self.workspace}/{self.repo} "
            f"(branch={self.branch!r}, "
            f"include_ext={self.include_extensions or 'all'}, "
            f"exclude_ext={self.exclude_extensions or 'none'}, "
            f"include_dir={self.include_directories or 'all'}, "
            f"exclude_dir={self.exclude_directories or 'none'})"
        )

    # ------------------------------------------------------------------
    # IngestionJob abstract method implementations
    # ------------------------------------------------------------------

    def list_items(self) -> Iterator[IngestionItem]:
        """Walk the repository tree and yield one IngestionItem per matching file.

        ``last_modified`` is always ``None``: the Bitbucket ``/src`` listing
        endpoint does not return per-file commit dates, so change-based dedup
        is not available for this connector.
        """
        logger.info(f"[{self.source_name}] Listing files in {self.workspace}/{self.repo}@{self.branch}")

        yielded = 0
        for entry in self._client.list_files(self.workspace, self.repo, self.branch):
            path: str = entry.get("path", "")
            if not path or not self._path_accepted(path):
                continue

            modified_str = entry.get("commit", {}).get("date", "") or ""
            last_modified = self._parse_timestamp(modified_str)

            yield IngestionItem(
                id=f"bitbucket:{self.workspace}/{self.repo}/{self.branch}/{path}",
                source_ref=path,
                last_modified=last_modified,
            )
            yielded += 1

        logger.info(f"[{self.source_name}] Found {yielded} file(s)")

    def get_raw_content(self, item: IngestionItem) -> str:
        """Fetch and return the raw file content from Bitbucket."""
        return self._client.get_file_content(self.workspace, self.repo, self.branch, item.source_ref)

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe identifier for the file."""
        raw = f"{self.workspace}_{self.repo}_{self.branch}_{item.source_ref}"
        safe = re.sub(r"[^\w\-.]", "_", raw)
        return safe[:255]

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        """Return Bitbucket-specific metadata fields."""
        path: str = item.source_ref
        ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""
        file_name = path.rsplit("/", 1)[-1] if "/" in path else path
        url = f"https://bitbucket.org/{self.workspace}/{self.repo}/src/{self.branch}/{path}"
        return {
            "url": url,
            "title": file_name,
            "workspace": self.workspace,
            "repo": self.repo,
            "branch": self.branch,
            "path": path,
            "file_extension": ext,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _path_accepted(self, path: str) -> bool:
        """Return True if the file path passes all include/exclude filters."""
        ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""

        if self.include_extensions and ext not in self.include_extensions:
            return False
        if self.exclude_extensions and ext in self.exclude_extensions:
            return False

        directory = path.rsplit("/", 1)[0] if "/" in path else ""

        if self.include_directories:
            if not any(directory == d or directory.startswith(d + "/") for d in self.include_directories):
                return False

        if self.exclude_directories:
            if any(directory == d or directory.startswith(d + "/") for d in self.exclude_directories):
                return False

        return True

    @staticmethod
    def _parse_csv(value: str) -> set[str]:
        """Parse a comma-separated string into a set of non-empty lowercase tokens."""
        if not value:
            return set()
        return {token.strip().lower() for token in value.split(",") if token.strip()}

    @staticmethod
    def _parse_timestamp(value: str) -> datetime | None:
        """Parse an ISO-8601 timestamp string into a datetime object."""
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            return None
