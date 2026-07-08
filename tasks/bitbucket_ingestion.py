# Standard library imports
import base64
import hashlib
import logging
from collections.abc import Iterator
from typing import Any
from urllib.parse import quote

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.filters import path_accepted
from utils.http import RetrySession
from utils.parse import parse_list, parse_timestamp
from utils.text import slugify

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _encoded_src_path(workspace: str, repo: str, branch: str, path: str = "") -> str:
    """Return the percent-encoded ``{workspace}/{repo}/src/{branch}/{path}`` segment.

    Shared by the API client (``BitbucketClient._src_url``) and the
    browser-facing citation URL (``BitbucketIngestionJob.get_extra_metadata``)
    so both stay percent-encoded consistently and can't drift apart.
    """
    enc_ws = quote(workspace, safe="")
    enc_repo = quote(repo, safe="")
    enc_branch = quote(branch, safe="")
    enc_path = quote(path, safe="/")
    return f"{enc_ws}/{enc_repo}/src/{enc_branch}/{enc_path}"


class BitbucketClient:
    """Thin HTTP client for the Bitbucket REST API v2.0.

    Handles authentication, request execution, and cursor-based pagination.
    """

    API_BASE = "https://api.bitbucket.org/2.0"

    def __init__(self, username: str, api_token: str) -> None:
        token = base64.b64encode(f"{username}:{api_token}".encode()).decode()
        self._headers = {"Authorization": f"Basic {token}"}
        self._session = RetrySession()

    def _src_url(self, workspace: str, repo: str, branch: str, path: str = "") -> str:
        return f"{self.API_BASE}/repositories/{_encoded_src_path(workspace, repo, branch, path)}"

    def list_files(self, workspace: str, repo: str, branch: str, path: str = "") -> Iterator[dict[str, Any]]:
        """Yield all ``commit_file`` entries from the repository, recursively.

        Paginates each directory listing via the ``next`` cursor and recurses
        into ``commit_directory`` entries.
        """
        url = self._src_url(workspace, repo, branch, path)
        params: dict[str, Any] = {"pagelen": 100}

        while url:
            try:
                resp = self._session.get(url, params=params, headers=self._headers)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(f"Bitbucket API error listing {path or '/'!r} in {workspace}/{repo}@{branch}: {e}")
                break

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
        url = self._src_url(workspace, repo, branch, path)
        try:
            resp = self._session.get(url, headers=self._headers)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
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

        self.include_extensions: set[str] = {
            f".{e}" if not e.startswith(".") else e for e in parse_list(include_ext, lower=True)
        }
        self.exclude_extensions: set[str] = {
            f".{e}" if not e.startswith(".") else e for e in parse_list(exclude_ext, lower=True)
        }
        self.include_directories: set[str] = set(parse_list(include_dir, lower=True))
        self.exclude_directories: set[str] = set(parse_list(exclude_dir, lower=True))

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
            if not path or not path_accepted(
                path,
                include_extensions=self.include_extensions or None,
                exclude_extensions=self.exclude_extensions or None,
                include_directories=self.include_directories or None,
                exclude_directories=self.exclude_directories or None,
            ):
                continue

            modified_str = entry.get("commit", {}).get("date", "") or ""
            last_modified = parse_timestamp(modified_str)

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
        suffix = "_" + hashlib.sha1(raw.encode(), usedforsecurity=False).hexdigest()[:8]
        return slugify(raw, max_len=255 - len(suffix)) + suffix

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        """Return Bitbucket-specific metadata fields."""
        path: str = item.source_ref
        ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""
        file_name = path.rsplit("/", 1)[-1] if "/" in path else path
        url = f"https://bitbucket.org/{_encoded_src_path(self.workspace, self.repo, self.branch, path)}"
        return {
            "url": url,
            "title": file_name,
            "workspace": self.workspace,
            "repo": self.repo,
            "branch": self.branch,
            "path": path,
            "file_extension": ext,
        }
