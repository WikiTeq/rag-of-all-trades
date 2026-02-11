import io
import logging
from typing import Iterable, Dict, Any
from jira import JIRA
from markitdown import MarkItDown

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)

class JiraIngestionJob(IngestionJob):
    def __init__(self, config: Dict[str, Any]):
        # super().__init__ triggers the framework's metadata and vector store setup
        super().__init__(config)
        self.md = MarkItDown()

        # Config extraction
        self.server = config.get("server")
        self.email = config.get("email")
        self.token = config.get("token")
        self.query = config.get("query")
        self.max_items = config.get("max_items", 50)
        self.load_comments = config.get("load_comments", False)
        self.max_comments = config.get("max_comments", 5)

        if not self.server or not self.query:
            raise ValueError("Configuration requires 'server' and 'query'.")

        # Authenticate once at startup
        self.jira_client = self._authenticate()

    def _authenticate(self) -> JIRA:
        if self.email and self.token:
            return JIRA(server=self.server, basic_auth=(self.email, self.token))
        return JIRA(server=self.server, token_auth=self.token)

    @property
    def source_type(self) -> str:
        return "jira"

    def list_items(self) -> Iterable[IngestionItem]:
        issues = self.jira_client.search_issues(self.query, maxResults=self.max_items)
        for issue in issues:
            # Pass arguments positionally: (id, reference, last_modified)
            # Try this order, which is standard for this framework:
            yield IngestionItem(
                issue.key, 
                f"{self.server}/browse/{issue.key}", 
                issue.fields.updated
            )

    def get_raw_content(self, item: IngestionItem) -> str:
        """Extraction: Converts one specific issue to Markdown."""
        issue = self.jira_client.issue(item.id)
        
        content = f"# {issue.key}: {issue.fields.summary}\n\n"
        content += f"## Description\n{issue.fields.description or ''}\n\n"

        if self.load_comments:
            content += "## Comments\n"
            comments = self.jira_client.comments(issue)
            for c in comments[:self.max_comments]:
                content += f"- **{getattr(c.author, 'displayName', 'User')}**: {c.body}\n"

        # Using your io.BytesIO technique
        md_stream = io.BytesIO(content.encode('utf-8'))
        md_result = self.md.convert_stream(md_stream, extension=".md")
        return md_result.text_content

    def get_item_name(self, item: IngestionItem) -> str:
        return f"jira_{item.id}.md"

    def get_document_metadata(self, item: IngestionItem, item_name: str, checksum: str, version: int, last_modified) -> Dict[str, Any]:
        """Metadata: Merges framework metadata with your 9 requested fields."""
        # Get standard metadata (version, checksum, etc) from base.py
        metadata = super().get_document_metadata(item, item_name, checksum, version, last_modified)
        
        # Fetch issue for detailed RAG metadata
        issue = self.jira_client.issue(item.id)
        
        metadata.update({
            "id": issue.key,
            "title": issue.fields.summary,
            "url": f"{self.server}/browse/{issue.key}",
            "assignee": getattr(issue.fields.assignee, "displayName", "Unassigned"),
            "status": issue.fields.status.name,
            "labels": getattr(issue.fields, "labels", []),
            "reporter": getattr(issue.fields.reporter, "displayName", "Unknown"),
            "project": issue.fields.project.name,
            "priority": getattr(issue.fields.priority, "name", "None"),
        })
        return metadata