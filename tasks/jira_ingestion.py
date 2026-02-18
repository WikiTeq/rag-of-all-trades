# Standard library imports
import logging
import re
from datetime import datetime
from typing import Any, Dict, Iterator, Optional

# Third-party imports
from jira import JIRA
from markitdown import MarkItDown

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)


class JiraIngestionJob(IngestionJob):
    """Ingestion connector for Jira Cloud and on-premise instances.

    Fetches issues via JQL, converts their content to Markdown, and stores
    them in the vector store. Supports Basic auth (email + API token) and
    Personal Access Token (PAT) auth.

    Bonus: Optionally fetches the top N comments per issue when
    ``load_comments`` is True in the connector config.

    Configuration (config.yaml):
        - config.server_url: Jira server URL (required)
        - config.jql: JQL query to select issues (required)
        - config.auth_type: "basic" or "token" (required)
        - config.email: User email for basic auth (required when auth_type=basic)
        - config.api_token: API token / PAT (required)
        - config.max_results: Maximum number of issues to fetch (optional, default 50)
        - config.load_comments: Whether to load issue comments (optional, default False)
        - config.max_comments: Maximum comments to include per issue (optional, default 10)
        - config.schedules: Celery schedule in seconds (optional)
    """

    @property
    def source_type(self) -> str:
        return "jira"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        # Required fields
        self.server_url = cfg.get("server_url", "").rstrip("/")
        if not self.server_url:
            raise ValueError("server_url is required in Jira connector config")

        self.jql = cfg.get("jql", "").strip()
        if not self.jql:
            raise ValueError("jql is required in Jira connector config")

        self.auth_type = cfg.get("auth_type", "").lower()
        if self.auth_type not in ("basic", "token"):
            raise ValueError(
                "auth_type must be 'basic' or 'token' in Jira connector config"
            )

        self.api_token = cfg.get("api_token", "").strip()
        if not self.api_token:
            raise ValueError("api_token is required in Jira connector config")

        if self.auth_type == "basic":
            self.email = cfg.get("email", "").strip()
            if not self.email:
                raise ValueError(
                    "email is required when auth_type is 'basic' in Jira connector config"
                )
        else:
            self.email = None

        # Optional fields
        self.max_results = int(cfg.get("max_results", 50))
        if self.max_results <= 0:
            raise ValueError("max_results must be positive")

        self.load_comments = bool(cfg.get("load_comments", False))
        self.max_comments = int(cfg.get("max_comments", 10))
        if self.max_comments <= 0:
            raise ValueError("max_comments must be positive")

        # Build authenticated JIRA client
        self._jira = self._build_client()
        self._md = MarkItDown()

        logger.info(
            f"Initialized Jira connector for {self.server_url} "
            f"(auth={self.auth_type}, jql={self.jql!r}, "
            f"max_results={self.max_results}, load_comments={self.load_comments})"
        )

    def _build_client(self) -> JIRA:
        """Construct an authenticated JIRA client."""
        if self.auth_type == "basic":
            return JIRA(
                server=self.server_url,
                basic_auth=(self.email, self.api_token),
            )
        else:
            # Personal Access Token — supported by Jira Server / Data Center
            # and by Atlassian Cloud when passed as a Bearer token.
            options = {
                "server": self.server_url,
                "headers": {"Authorization": f"Bearer {self.api_token}"},
            }
            return JIRA(options=options)

    # ------------------------------------------------------------------
    # IngestionJob abstract method implementations
    # ------------------------------------------------------------------

    def list_items(self) -> Iterator[IngestionItem]:
        """Query Jira with the configured JQL and yield one IngestionItem per issue.

        Paginates automatically until max_results is reached or all matching
        issues have been returned.
        """
        logger.info(
            f"[{self.source_name}] Listing issues with JQL: {self.jql!r}"
        )

        page_size = min(
            100, self.max_results
        )  # Jira Cloud caps at 100 per request
        start_at = 0
        fetched = 0

        while fetched < self.max_results:
            batch_limit = min(page_size, self.max_results - fetched)
            try:
                issues = self._jira.search_issues(
                    self.jql,
                    startAt=start_at,
                    maxResults=batch_limit,
                    fields="summary,description,status,assignee,reporter,labels,project,priority,issuetype,updated,created,comment",
                )
            except Exception as e:
                logger.error(
                    f"[{self.source_name}] Failed to search issues: {e}"
                )
                break

            if not issues:
                break

            for issue in issues:
                updated_at = self._parse_jira_timestamp(
                    getattr(issue.fields, "updated", None)
                )
                yield IngestionItem(
                    id=f"jira:{issue.key}",
                    source_ref=issue,
                    last_modified=updated_at,
                )
                fetched += 1
                if fetched >= self.max_results:
                    break

            # Stop paginating if we got fewer results than requested
            if len(issues) < batch_limit:
                break

            start_at += len(issues)

        logger.info(f"[{self.source_name}] Found {fetched} issue(s)")

    def get_raw_content(self, item: IngestionItem) -> str:
        """Build Markdown-formatted content from a Jira issue.

        Converts summary + description to Markdown, then appends the top N
        comments if comment loading is enabled. Caches the issue URL in the
        item's metadata cache for use in get_document_metadata().
        """
        issue = item.source_ref

        # Cache issue URL for metadata
        permalink = issue.permalink()
        item._metadata_cache["issue_url"] = permalink

        parts: list[str] = []

        summary = getattr(issue.fields, "summary", "") or ""
        parts.append(f"# {summary}\n")

        description = getattr(issue.fields, "description", "") or ""
        if description.strip():
            md_description = self._to_markdown(description)
            if md_description.strip():
                parts.append(md_description)

        if self.load_comments:
            comments_md = self._build_comments_section(issue)
            if comments_md:
                parts.append(comments_md)

        return "\n\n".join(parts)

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe identifier for the issue (e.g. ``PROJ-123``)."""
        issue = item.source_ref
        key = getattr(issue, "key", "") or item.id
        safe = re.sub(r"[^\w\-]", "_", key)
        return safe[:255]

    def get_document_metadata(
        self,
        item: IngestionItem,
        item_name: str,
        checksum: str,
        version: int,
        last_modified: Any,
    ) -> Dict[str, Any]:
        """Build metadata dict with all required Jira-specific fields."""
        issue = item.source_ref
        fields = issue.fields

        metadata = super().get_document_metadata(
            item, item_name, checksum, version, last_modified
        )

        # Extend with Jira-specific fields
        metadata.update(
            {
                "url": item._metadata_cache.get("issue_url", ""),
                "title": getattr(fields, "summary", "") or "",
                "id": issue.id,
                "assignee": self._safe_display_name(
                    getattr(fields, "assignee", None)
                ),
                "reporter": self._safe_display_name(
                    getattr(fields, "reporter", None)
                ),
                "status": self._safe_get(fields, "status", "name") or "",
                "labels": list(getattr(fields, "labels", []) or []),
                "project": self._safe_get(fields, "project", "name") or "",
                "priority": self._safe_get(fields, "priority", "name") or "",
            }
        )
        return metadata

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _to_markdown(self, text: str) -> str:
        """Convert Jira description text to Markdown using MarkItDown.

        Falls back to returning the original text if conversion fails.
        Jira descriptions may use Jira wiki markup or Atlassian Document Format
        (ADF, a JSON structure). Both are handled gracefully here.
        """
        # ADF is a JSON dict — MarkItDown won't help; extract plain text
        if isinstance(text, dict):
            return self._extract_adf_text(text)

        if not text or not text.strip():
            return ""

        try:
            import io

            result = self._md.convert_stream(io.BytesIO(text.encode("utf-8")))
            converted = result.text_content or ""
            return converted.strip() if converted.strip() else text
        except Exception:
            return text

    def _extract_adf_text(self, adf: dict) -> str:
        """Recursively extract plain text from an Atlassian Document Format (ADF) node."""
        text_parts: list[str] = []

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                node_type = node.get("type", "")
                # Text leaf node
                if node_type == "text":
                    text_parts.append(node.get("text", ""))
                    return
                # Heading — prepend Markdown '#' markers
                if node_type == "heading":
                    level = node.get("attrs", {}).get("level", 1)
                    prefix = "#" * level + " "
                    for child in node.get("content", []):
                        if child.get("type") == "text":
                            text_parts.append(prefix + child.get("text", ""))
                    return
                for child in node.get("content", []):
                    walk(child)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(adf)
        return "\n".join(text_parts)

    def _build_comments_section(self, issue: Any) -> str:
        """Fetch and format the top N comments for an issue as Markdown."""
        try:
            comments = self._jira.comments(issue)
        except Exception as e:
            logger.warning(
                f"[{self.source_name}] Failed to fetch comments for {issue.key}: {e}"
            )
            return ""

        if not comments:
            return ""

        top_comments = comments[: self.max_comments]
        lines: list[str] = ["## Comments"]
        for comment in top_comments:
            author = self._safe_display_name(getattr(comment, "author", None))
            created = getattr(comment, "created", "") or ""
            body = getattr(comment, "body", "") or ""
            if isinstance(body, dict):
                body = self._extract_adf_text(body)
            lines.append(f"**{author}** ({created}):\n{body}")

        return "\n\n".join(lines)

    @staticmethod
    def _safe_display_name(obj: Any) -> str:
        """Extract ``displayName`` from a Jira user object, or return empty string."""
        if obj is None:
            return ""
        return getattr(obj, "displayName", "") or ""

    @staticmethod
    def _safe_get(fields: Any, attr: str, sub_attr: str) -> Optional[str]:
        """Safely navigate two levels of attribute access."""
        obj = getattr(fields, attr, None)
        if obj is None:
            return None
        return getattr(obj, sub_attr, None)

    @staticmethod
    def _parse_jira_timestamp(value: Optional[str]) -> Optional[datetime]:
        """Parse a Jira ISO-8601 timestamp string into a datetime object."""
        if not value:
            return None
        try:
            # Jira returns e.g. "2024-01-15T10:30:00.000+0000"
            # Python's fromisoformat handles this in 3.11+
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            return None
