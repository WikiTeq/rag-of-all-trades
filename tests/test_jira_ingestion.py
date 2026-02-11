import unittest
from unittest.mock import MagicMock, patch
from tasks.jira_ingestion import JiraIngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

class TestJiraIngestion(unittest.TestCase):
    def setUp(self):
        # Added 'name' as their base.py requires it for source_name
        self.config = {
            "name": "jira_test_source",
            "server": "https://test.atlassian.net",
            "token": "fake_token",
            "query": "project = TEST",
            "load_comments": True,
            "max_comments": 2
        }

    @patch("tasks.jira_ingestion.JIRA")
    def test_list_items(self, MockJira):
        mock_issue = MagicMock()
        mock_issue.key = "TEST-1"
        mock_issue.fields.updated = "2024-01-01"
        
        MockJira.return_value.search_issues.return_value = [mock_issue]
        
        job = JiraIngestionJob(self.config)
        items = list(job.list_items())
        
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "TEST-1")

    @patch("tasks.jira_ingestion.JIRA")
    def test_get_raw_content(self, MockJira):
        mock_issue = MagicMock()
        mock_issue.key = "TEST-1"
        mock_issue.fields.summary = "Test Title"
        mock_issue.fields.description = "Test Desc"
        
        MockJira.return_value.issue.return_value = mock_issue
        
        job = JiraIngestionJob(self.config)
        item = IngestionItem("TEST-1", "url", "now")
        content = job.get_raw_content(item)
        
        self.assertIn("# TEST-1", content)
        self.assertIn("Test Title", content)

if __name__ == "__main__":
    unittest.main()