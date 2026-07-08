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


def _dir_excluded(dir_path: str, exclude_directories: set[str]) -> bool:
    """Return True if ``dir_path`` is at or under one of the excluded prefixes.

    Mirrors the directory prefix-matching rule in ``utils.filters.path_accepted``
    so crawl-time pruning agrees with the file-level filter.
    """
    norm = dir_path.replace("\\", "/")
    return any(norm.startswith(d.rstrip("/") + "/") or norm == d for d in exclude_directories)


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

    def list_files(
        self,
        workspace: str,
        repo: str,
        branch: str,
        path: str = "",
        *,
        exclude_directories: set[str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield all ``commit_file`` entries from the repository, recursively.

        Paginates each directory listing via the ``next`` cursor and recurses
        into ``commit_directory`` entries. When ``exclude_directories`` is
        given, recursion is pruned for any subdirectory whose path matches
        one of the given prefixes (same prefix-matching rule as
        ``utils.filters.path_accepted``), avoiding HTTP calls for excluded
        subtrees entirely.
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
                    if not dir_path:
                        continue
                    if exclude_directories and _dir_excluded(dir_path, exclude_directories):
                        continue
                    yield from self.list_files(
                        workspace, repo, branch, dir_path, exclude_directories=exclude_directories
                    )

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

    def get_file_commit_hash(self, workspace: str, repo: str, branch: str, path: str) -> str | None:
        """Return the hash of the commit that last touched ``path``, without fetching its content.

        Uses the ``filehistory`` endpoint, which returns per-file metadata
        (verified against the live API: distinct hashes per file, unlike
        ``?format=meta``'s ``commit.hash``, which is the branch-ref commit —
        identical for every file and useless as a per-file signal). The
        response body is small regardless of the file's own size, so this
        is cheap even for large files, unlike a full content ``GET``.

        Returns None on any error or an empty/missing history — callers
        should treat that as "no cheap checksum available" and fall back
        to full-content MD5.
        """
        enc_ws = quote(workspace, safe="")
        enc_repo = quote(repo, safe="")
        enc_branch = quote(branch, safe="")
        enc_path = quote(path, safe="/")
        url = f"{self.API_BASE}/repositories/{enc_ws}/{enc_repo}/filehistory/{enc_branch}/{enc_path}"
        try:
            resp = self._session.get(url, headers=self._headers)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"Bitbucket API error resolving filehistory for {path!r} in {workspace}/{repo}: {e}")
            return None

        values = data.get("values") or []
        if not values:
            return None
        return (values[0].get("commit") or {}).get("hash")

    def get_commit_date(self, workspace: str, repo: str, commit_hash: str) -> str | None:
        """Return the ISO-8601 ``date`` of the given commit.

        Neither the directory listing nor ``filehistory`` include a per-file
        date (verified against the live API), so this is a second request
        against the commit resource itself, using a hash already obtained
        from ``get_file_commit_hash()``. Returns None on any error.
        """
        enc_ws = quote(workspace, safe="")
        enc_repo = quote(repo, safe="")
        enc_hash = quote(commit_hash, safe="")
        url = f"{self.API_BASE}/repositories/{enc_ws}/{enc_repo}/commit/{enc_hash}"
        try:
            resp = self._session.get(url, headers=self._headers)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"Bitbucket API error resolving commit date for {commit_hash!r} in {workspace}/{repo}: {e}")
            return None
        return data.get("date")

    def get_default_branch(self, workspace: str, repo: str) -> str:
        """Return the repository's configured default branch (``mainbranch.name``).

        Raises RuntimeError if the repository lookup fails or the response
        has no ``mainbranch.name`` — callers should treat this as a hard
        config error rather than silently falling back to a guessed branch.
        """
        enc_ws = quote(workspace, safe="")
        enc_repo = quote(repo, safe="")
        url = f"{self.API_BASE}/repositories/{enc_ws}/{enc_repo}"
        try:
            resp = self._session.get(url, headers=self._headers)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            raise RuntimeError(f"Bitbucket API error resolving default branch for {workspace}/{repo}: {e}") from e

        name = (data.get("mainbranch") or {}).get("name")
        if not name:
            raise RuntimeError(f"Bitbucket repository {workspace}/{repo} has no mainbranch.name in its response")
        return name


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
        - config.branch: Branch or ref to ingest (optional; when omitted, the
          repository's actual default branch is resolved via the Bitbucket
          API at init time — no hardcoded fallback)
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

        configured_branch = cfg.get("branch", "").strip()

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

        if configured_branch:
            self.branch = configured_branch
        else:
            try:
                self.branch = self._client.get_default_branch(self.workspace, self.repo)
            except RuntimeError as e:
                raise ValueError(
                    f"branch was not set in Bitbucket connector config and default-branch detection failed: {e}"
                ) from e

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

        When ``include_directories`` is set, the crawl starts at each included
        prefix directly instead of walking the whole repository — this avoids
        HTTP calls for subtrees that would be filtered out anyway. When
        ``exclude_directories`` is set, the crawl starts at the root but
        prunes recursion into excluded subtrees. Extension-only filters still
        require a full walk of the resulting subtree(s), applied via
        ``path_accepted()`` below.

        Entries whose listing ``attributes`` include ``"binary"`` are skipped —
        binary files (images, PDFs, etc.) would otherwise be decoded as text
        via ``resp.text`` in ``get_raw_content()`` and produce garbage content
        that still passes the empty-content check.

        ``last_modified`` is resolved per-file via the ``filehistory`` and
        commit-resource endpoints (neither the ``/src`` listing nor
        ``filehistory`` itself include a per-file date — verified against
        the live API). The commit hash resolved here is cached on the
        item so ``get_item_checksum()`` can reuse it instead of calling
        ``filehistory`` a second time for the same file. Falls back to
        ``None`` (and the base class's ``datetime.now()`` default) if either
        lookup fails.
        """
        logger.info(f"[{self.source_name}] Listing files in {self.workspace}/{self.repo}@{self.branch}")

        yielded = 0
        seen_paths: set[str] = set()
        for entry in self._iter_entries():
            path: str = entry.get("path", "")
            if not path or path in seen_paths:
                continue
            seen_paths.add(path)

            if not path_accepted(
                path,
                include_extensions=self.include_extensions or None,
                exclude_extensions=self.exclude_extensions or None,
                include_directories=self.include_directories or None,
                exclude_directories=self.exclude_directories or None,
            ):
                continue

            if "binary" in (entry.get("attributes") or []):
                logger.info(f"[{self.source_name}] Skipping binary file: {path}")
                continue

            commit_hash = self._client.get_file_commit_hash(self.workspace, self.repo, self.branch, path)
            last_modified = None
            if commit_hash:
                date_str = self._client.get_commit_date(self.workspace, self.repo, commit_hash)
                last_modified = parse_timestamp(date_str)

            item = IngestionItem(
                id=f"bitbucket:{self.workspace}/{self.repo}/{self.branch}/{path}",
                source_ref=path,
                last_modified=last_modified,
            )
            item._metadata_cache["commit_hash"] = commit_hash
            yield item
            yielded += 1

        logger.info(f"[{self.source_name}] Found {yielded} file(s)")

    def _iter_entries(self) -> Iterator[dict[str, Any]]:
        """Crawl the repository, scoped to ``include_directories`` when set.

        With ``include_directories``, issues one crawl per included prefix
        instead of a single root crawl, so excluded subtrees are never
        listed. Without it, crawls from the root and prunes recursion into
        any ``exclude_directories`` subtree.
        """
        if self.include_directories:
            for prefix in sorted(self.include_directories):
                yield from self._client.list_files(self.workspace, self.repo, self.branch, prefix)
            return

        yield from self._client.list_files(
            self.workspace,
            self.repo,
            self.branch,
            exclude_directories=self.exclude_directories or None,
        )

    def get_item_checksum(self, item: IngestionItem) -> str | None:
        """Return the hash of the commit that last touched this file, without fetching its content.

        Reuses the commit hash already resolved and cached on the item by
        ``list_items()`` (it needs the same hash to look up ``last_modified``,
        so caching here avoids calling ``filehistory`` a second time for the
        same file). Falls back to a fresh lookup if the cache is empty for
        any reason — e.g. an item constructed outside ``list_items()``.

        Unlike MediaWiki's ``lastrevid`` (already present in its page listing,
        so its checksum is free), Bitbucket's file listing carries no
        per-file revision — this makes one ``filehistory`` API call per item
        (shared with the ``last_modified`` lookup). That call's response is
        small and constant-size regardless of the file's own size, so it is
        still cheaper than the full-content ``GET`` + MD5 path it replaces,
        especially for larger files.

        Verified against the live Bitbucket API that ``?format=meta``'s
        ``commit.hash`` is the branch-ref commit (identical for every file —
        useless as a per-file signal), while ``filehistory``'s commit hash
        is genuinely per-file. See PR52-fixes.md for details.

        Returns None on any error, falling back to content-based MD5.
        """
        cached_hash = item._metadata_cache.get("commit_hash")
        if cached_hash:
            return cached_hash
        return self._client.get_file_commit_hash(self.workspace, self.repo, self.branch, item.source_ref)

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
