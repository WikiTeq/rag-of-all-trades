import unittest
from datetime import datetime
from unittest.mock import Mock, patch

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.jira_ingestion import JiraIngestionJob

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(
    auth_type="basic",
    email="user@example.com",
    api_token="secret",
    jql="project = TEST",
    server_url="https://jira.example.com",
    max_results=50,
    load_comments=False,
    max_comments=10,
):
    cfg = {
        "server_url": server_url,
        "auth_type": auth_type,
        "api_token": api_token,
        "jql": jql,
        "max_results": max_results,
        "load_comments": load_comments,
        "max_comments": max_comments,
    }
    if auth_type == "basic":
        cfg["email"] = email
    return {"name": "test_jira", "config": cfg}


def _make_issue(
    key="TEST-1",
    issue_id="10001",
    summary="Test Issue",
    description="Issue description",
    status="Open",
    assignee_name="Alice",
    reporter_name="Bob",
    labels=None,
    project_name="Test Project",
    priority_name="Medium",
    updated="2024-06-01T12:00:00.000+0000",
    created="2024-05-01T08:00:00.000+0000",
    permalink="https://jira.example.com/browse/TEST-1",
):
    issue = Mock()
    issue.key = key
    issue.id = issue_id
    issue.permalink.return_value = permalink

    fields = Mock()
    fields.summary = summary
    fields.description = description
    fields.updated = updated
    fields.created = created
    fields.labels = labels or []

    status_obj = Mock()
    status_obj.name = status
    fields.status = status_obj

    assignee = Mock()
    assignee.displayName = assignee_name
    fields.assignee = assignee

    reporter = Mock()
    reporter.displayName = reporter_name
    fields.reporter = reporter

    project = Mock()
    project.name = project_name
    fields.project = project

    priority = Mock()
    priority.name = priority_name
    fields.priority = priority

    issue.fields = fields
    return issue


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class TestJiraIngestionJob(unittest.TestCase):
    def setUp(self):
        self.jira_patcher = patch("tasks.jira_ingestion.JIRA")
        self.markitdown_patcher = patch("tasks.jira_ingestion.MarkItDown")
        self.mock_jira_class = self.jira_patcher.start()
        self.mock_md_class = self.markitdown_patcher.start()

        self.mock_jira = Mock()
        self.mock_jira_class.return_value = self.mock_jira

        self.mock_md = Mock()
        self.mock_md_class.return_value = self.mock_md

    def tearDown(self):
        self.jira_patcher.stop()
        self.markitdown_patcher.stop()

    def _make_job(self, **kwargs):
        return JiraIngestionJob(_make_config(**kwargs))

    # ------------------------------------------------------------------
    # Initialisation & validation
    # ------------------------------------------------------------------

    def test_source_type(self):
        job = self._make_job()
        self.assertEqual(job.source_type, "jira")

    def test_basic_auth_creates_jira_client_with_basic_auth(self):
        self._make_job(
            auth_type="basic", email="u@example.com", api_token="tok"
        )
        self.mock_jira_class.assert_called_once_with(
            server="https://jira.example.com",
            basic_auth=("u@example.com", "tok"),
        )

    def test_token_auth_creates_jira_client_with_bearer_header(self):
        self._make_job(auth_type="token", api_token="myPAT")
        self.mock_jira_class.assert_called_once_with(
            options={
                "server": "https://jira.example.com",
                "headers": {"Authorization": "Bearer myPAT"},
            }
        )

    def test_server_url_trailing_slash_is_stripped(self):
        self._make_job(server_url="https://jira.example.com/")
        call_kwargs = self.mock_jira_class.call_args
        # For basic auth the server kwarg is used
        self.assertEqual(
            call_kwargs.kwargs["server"], "https://jira.example.com"
        )

    def test_missing_server_url_raises(self):
        with self.assertRaises(ValueError):
            JiraIngestionJob(
                {
                    "name": "x",
                    "config": {
                        "auth_type": "basic",
                        "email": "a@b.com",
                        "api_token": "t",
                        "jql": "project = X",
                    },
                }
            )

    def test_missing_jql_raises(self):
        with self.assertRaises(ValueError):
            JiraIngestionJob(
                {
                    "name": "x",
                    "config": {
                        "server_url": "https://jira.example.com",
                        "auth_type": "basic",
                        "email": "a@b.com",
                        "api_token": "t",
                    },
                }
            )

    def test_invalid_auth_type_raises(self):
        with self.assertRaises(ValueError):
            JiraIngestionJob(
                {
                    "name": "x",
                    "config": {
                        "server_url": "https://jira.example.com",
                        "auth_type": "oauth",
                        "api_token": "t",
                        "jql": "project = X",
                    },
                }
            )

    def test_missing_api_token_raises(self):
        with self.assertRaises(ValueError):
            JiraIngestionJob(
                {
                    "name": "x",
                    "config": {
                        "server_url": "https://jira.example.com",
                        "auth_type": "basic",
                        "email": "a@b.com",
                        "jql": "project = X",
                    },
                }
            )

    def test_basic_auth_missing_email_raises(self):
        with self.assertRaises(ValueError):
            JiraIngestionJob(
                {
                    "name": "x",
                    "config": {
                        "server_url": "https://jira.example.com",
                        "auth_type": "basic",
                        "api_token": "t",
                        "jql": "project = X",
                    },
                }
            )

    def test_non_positive_max_results_raises(self):
        with self.assertRaises(ValueError):
            self._make_job(max_results=0)

    def test_non_positive_max_comments_raises(self):
        with self.assertRaises(ValueError):
            self._make_job(load_comments=True, max_comments=0)

    # ------------------------------------------------------------------
    # list_items
    # ------------------------------------------------------------------

    def test_list_items_yields_ingestion_items(self):
        issue1 = _make_issue(key="TEST-1")
        issue2 = _make_issue(
            key="TEST-2", updated="2024-07-01T00:00:00.000+0000"
        )
        self.mock_jira.search_issues.return_value = [issue1, issue2]

        job = self._make_job()
        items = list(job.list_items())

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].id, "jira:TEST-1")
        self.assertEqual(items[1].id, "jira:TEST-2")
        self.assertIsInstance(items[0], IngestionItem)
        # source_ref is the raw issue object
        self.assertIs(items[0].source_ref, issue1)

    def test_list_items_last_modified_parsed(self):
        issue = _make_issue(updated="2024-06-15T10:30:00.000+0000")
        self.mock_jira.search_issues.return_value = [issue]

        job = self._make_job()
        items = list(job.list_items())

        self.assertIsNotNone(items[0].last_modified)
        self.assertEqual(items[0].last_modified.year, 2024)
        self.assertEqual(items[0].last_modified.month, 6)
        self.assertEqual(items[0].last_modified.day, 15)

    def test_list_items_respects_max_results(self):
        # Return 5 issues but max_results=3
        issues = [_make_issue(key=f"TEST-{i}") for i in range(5)]
        self.mock_jira.search_issues.return_value = issues

        job = self._make_job(max_results=3)
        items = list(job.list_items())

        self.assertEqual(len(items), 3)

    def test_list_items_paginates_until_exhausted(self):
        # page_size = min(100, max_results=200) = 100
        # batch1 must equal batch_limit (100) to trigger a second request
        batch1 = [_make_issue(key=f"TEST-{i}") for i in range(100)]
        # batch2 returns fewer than 100 → pagination stops
        batch2 = [_make_issue(key="TEST-100")]
        self.mock_jira.search_issues.side_effect = [batch1, batch2]

        job = self._make_job(max_results=200)
        items = list(job.list_items())

        self.assertEqual(len(items), 101)
        self.assertEqual(self.mock_jira.search_issues.call_count, 2)
        second_call_kwargs = self.mock_jira.search_issues.call_args_list[
            1
        ].kwargs
        self.assertEqual(second_call_kwargs["startAt"], 100)

    def test_list_items_empty_result(self):
        self.mock_jira.search_issues.return_value = []

        job = self._make_job()
        items = list(job.list_items())

        self.assertEqual(items, [])

    def test_list_items_api_error_yields_nothing(self):
        self.mock_jira.search_issues.side_effect = Exception("API error")

        job = self._make_job()
        items = list(job.list_items())

        self.assertEqual(items, [])

    # ------------------------------------------------------------------
    # get_item_name
    # ------------------------------------------------------------------

    def test_get_item_name_returns_issue_key(self):
        issue = _make_issue(key="PROJ-42")
        item = IngestionItem(id="jira:PROJ-42", source_ref=issue)

        job = self._make_job()
        name = job.get_item_name(item)

        self.assertEqual(name, "PROJ-42")

    def test_get_item_name_sanitizes_special_chars(self):
        issue = _make_issue(key="MY PROJECT/42")
        item = IngestionItem(id="jira:MY PROJECT/42", source_ref=issue)

        job = self._make_job()
        name = job.get_item_name(item)

        self.assertNotIn(" ", name)
        self.assertNotIn("/", name)

    def test_get_item_name_truncates_to_255(self):
        issue = _make_issue(key="A" * 300)
        item = IngestionItem(id="jira:long", source_ref=issue)

        job = self._make_job()
        name = job.get_item_name(item)

        self.assertLessEqual(len(name), 255)

    # ------------------------------------------------------------------
    # get_raw_content
    # ------------------------------------------------------------------

    def test_get_raw_content_returns_markdown_with_summary_and_description(
        self,
    ):
        issue = _make_issue(
            summary="My Issue", description="Some description text"
        )
        item = IngestionItem(id="jira:TEST-1", source_ref=issue)

        # MarkItDown passes text through (simulate)
        md_result = Mock()
        md_result.text_content = "Some description text"
        self.mock_md.convert_stream.return_value = md_result

        job = self._make_job()
        content = job.get_raw_content(item)

        self.assertIn("My Issue", content)
        self.assertIn("Some description text", content)

    def test_get_raw_content_caches_issue_url(self):
        issue = _make_issue(permalink="https://jira.example.com/browse/TEST-1")
        item = IngestionItem(id="jira:TEST-1", source_ref=issue)

        md_result = Mock()
        md_result.text_content = "desc"
        self.mock_md.convert_stream.return_value = md_result

        job = self._make_job()
        job.get_raw_content(item)

        self.assertEqual(
            item._metadata_cache.get("issue_url"),
            "https://jira.example.com/browse/TEST-1",
        )

    def test_get_raw_content_empty_description(self):
        issue = _make_issue(summary="No body issue", description="")
        item = IngestionItem(id="jira:TEST-1", source_ref=issue)

        job = self._make_job()
        content = job.get_raw_content(item)

        self.assertIn("No body issue", content)

    def test_get_raw_content_none_description(self):
        issue = _make_issue(summary="No body issue", description=None)
        item = IngestionItem(id="jira:TEST-1", source_ref=issue)

        job = self._make_job()
        # Should not raise
        content = job.get_raw_content(item)
        self.assertIn("No body issue", content)

    def test_get_raw_content_includes_comments_when_enabled(self):
        issue = _make_issue(summary="Issue with comments", description="desc")
        item = IngestionItem(id="jira:TEST-1", source_ref=issue)

        md_result = Mock()
        md_result.text_content = "desc"
        self.mock_md.convert_stream.return_value = md_result

        comment = Mock()
        comment.author = Mock(displayName="Charlie")
        comment.created = "2024-06-01T10:00:00.000+0000"
        comment.body = "Great issue!"
        self.mock_jira.comments.return_value = [comment]

        job = self._make_job(load_comments=True, max_comments=5)
        content = job.get_raw_content(item)

        self.assertIn("Comments", content)
        self.assertIn("Charlie", content)
        self.assertIn("Great issue!", content)

    def test_get_raw_content_no_comments_when_disabled(self):
        issue = _make_issue(summary="Issue", description="desc")
        item = IngestionItem(id="jira:TEST-1", source_ref=issue)

        md_result = Mock()
        md_result.text_content = "desc"
        self.mock_md.convert_stream.return_value = md_result

        job = self._make_job(load_comments=False)
        job.get_raw_content(item)

        self.mock_jira.comments.assert_not_called()

    def test_get_raw_content_limits_comments_to_max_comments(self):
        issue = _make_issue(summary="Issue", description="desc")
        item = IngestionItem(id="jira:TEST-1", source_ref=issue)

        md_result = Mock()
        md_result.text_content = "desc"
        self.mock_md.convert_stream.return_value = md_result

        # 5 comments, but max_comments=2
        comments = []
        for i in range(5):
            c = Mock()
            c.author = Mock(displayName=f"User{i}")
            c.created = "2024-06-01T10:00:00.000+0000"
            c.body = f"Comment {i}"
            comments.append(c)
        self.mock_jira.comments.return_value = comments

        job = self._make_job(load_comments=True, max_comments=2)
        content = job.get_raw_content(item)

        self.assertIn("Comment 0", content)
        self.assertIn("Comment 1", content)
        self.assertNotIn("Comment 2", content)

    def test_get_raw_content_comment_fetch_failure_does_not_raise(self):
        issue = _make_issue(summary="Issue", description="desc")
        item = IngestionItem(id="jira:TEST-1", source_ref=issue)

        md_result = Mock()
        md_result.text_content = "desc"
        self.mock_md.convert_stream.return_value = md_result

        self.mock_jira.comments.side_effect = Exception("403 Forbidden")

        job = self._make_job(load_comments=True)
        # Should not raise; comments section just omitted
        content = job.get_raw_content(item)
        self.assertIn("Issue", content)

    # ------------------------------------------------------------------
    # get_document_metadata
    # ------------------------------------------------------------------

    def test_get_document_metadata_contains_all_required_fields(self):
        issue = _make_issue(
            key="TEST-1",
            issue_id="10001",
            summary="My Issue",
            status="In Progress",
            assignee_name="Alice",
            reporter_name="Bob",
            labels=["bug", "urgent"],
            project_name="Test Project",
            priority_name="High",
        )
        item = IngestionItem(
            id="jira:TEST-1",
            source_ref=issue,
            last_modified=datetime(2024, 6, 1, 12, 0, 0),
        )
        object.__setattr__(
            item,
            "_metadata_cache",
            {"issue_url": "https://jira.example.com/browse/TEST-1"},
        )

        job = self._make_job()
        metadata = job.get_document_metadata(
            item=item,
            item_name="TEST-1",
            checksum="abc123",
            version=1,
            last_modified=datetime(2024, 6, 1, 12, 0, 0),
        )

        # Base fields from IngestionJob
        self.assertEqual(metadata["source"], "jira")
        self.assertEqual(metadata["key"], "TEST-1")
        self.assertEqual(metadata["checksum"], "abc123")
        self.assertEqual(metadata["version"], 1)
        self.assertEqual(metadata["format"], "markdown")
        self.assertEqual(metadata["source_name"], "test_jira")

        # Jira-specific fields
        self.assertEqual(metadata["id"], "10001")
        self.assertEqual(metadata["title"], "My Issue")
        self.assertEqual(
            metadata["url"], "https://jira.example.com/browse/TEST-1"
        )
        self.assertEqual(metadata["assignee"], "Alice")
        self.assertEqual(metadata["reporter"], "Bob")
        self.assertEqual(metadata["status"], "In Progress")
        self.assertEqual(metadata["labels"], ["bug", "urgent"])
        self.assertEqual(metadata["project"], "Test Project")
        self.assertEqual(metadata["priority"], "High")

    def test_get_document_metadata_handles_unassigned_issue(self):
        issue = _make_issue(key="TEST-2")
        issue.fields.assignee = None
        issue.fields.reporter = None
        item = IngestionItem(
            id="jira:TEST-2",
            source_ref=issue,
            last_modified=datetime(2024, 6, 1),
        )
        object.__setattr__(item, "_metadata_cache", {"issue_url": ""})

        job = self._make_job()
        metadata = job.get_document_metadata(
            item=item,
            item_name="TEST-2",
            checksum="x",
            version=1,
            last_modified=datetime(2024, 6, 1),
        )

        self.assertEqual(metadata["assignee"], "")
        self.assertEqual(metadata["reporter"], "")

    def test_get_document_metadata_missing_url_cache_uses_empty_string(self):
        issue = _make_issue(key="TEST-3")
        item = IngestionItem(id="jira:TEST-3", source_ref=issue)
        # No url in _metadata_cache

        job = self._make_job()
        metadata = job.get_document_metadata(
            item=item,
            item_name="TEST-3",
            checksum="x",
            version=1,
            last_modified=None,
        )

        self.assertEqual(metadata["url"], "")

    # ------------------------------------------------------------------
    # _to_markdown helpers
    # ------------------------------------------------------------------

    def test_to_markdown_falls_back_on_empty_conversion(self):
        md_result = Mock()
        md_result.text_content = "   "
        self.mock_md.convert_stream.return_value = md_result

        job = self._make_job()
        result = job._to_markdown("original text")

        self.assertEqual(result, "original text")

    def test_to_markdown_falls_back_on_conversion_error(self):
        self.mock_md.convert_stream.side_effect = ValueError("bad")

        job = self._make_job()
        result = job._to_markdown("original text")

        self.assertEqual(result, "original text")

    def test_to_markdown_returns_empty_for_blank_input(self):
        job = self._make_job()
        self.assertEqual(job._to_markdown(""), "")
        self.assertEqual(job._to_markdown("   "), "")

    def test_to_markdown_handles_adf_dict(self):
        adf = {
            "type": "doc",
            "content": [
                {
                    "type": "paragraph",
                    "content": [{"type": "text", "text": "Hello ADF"}],
                }
            ],
        }
        job = self._make_job()
        result = job._to_markdown(adf)
        self.assertIn("Hello ADF", result)

    # ------------------------------------------------------------------
    # _extract_adf_text
    # ------------------------------------------------------------------

    def test_extract_adf_text_simple_paragraph(self):
        adf = {
            "type": "doc",
            "content": [
                {
                    "type": "paragraph",
                    "content": [{"type": "text", "text": "Simple text"}],
                }
            ],
        }
        job = self._make_job()
        result = job._extract_adf_text(adf)
        self.assertIn("Simple text", result)

    def test_extract_adf_text_heading(self):
        adf = {
            "type": "doc",
            "content": [
                {
                    "type": "heading",
                    "attrs": {"level": 2},
                    "content": [{"type": "text", "text": "Section Title"}],
                }
            ],
        }
        job = self._make_job()
        result = job._extract_adf_text(adf)
        self.assertIn("## Section Title", result)

    def test_extract_adf_text_nested(self):
        adf = {
            "type": "doc",
            "content": [
                {
                    "type": "bulletList",
                    "content": [
                        {
                            "type": "listItem",
                            "content": [
                                {
                                    "type": "paragraph",
                                    "content": [
                                        {"type": "text", "text": "Item one"}
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ],
        }
        job = self._make_job()
        result = job._extract_adf_text(adf)
        self.assertIn("Item one", result)

    # ------------------------------------------------------------------
    # _parse_jira_timestamp
    # ------------------------------------------------------------------

    def test_parse_jira_timestamp_valid(self):
        result = JiraIngestionJob._parse_jira_timestamp(
            "2024-06-15T10:30:00.000+0000"
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.year, 2024)
        self.assertEqual(result.month, 6)
        self.assertEqual(result.day, 15)

    def test_parse_jira_timestamp_none_returns_none(self):
        self.assertIsNone(JiraIngestionJob._parse_jira_timestamp(None))

    def test_parse_jira_timestamp_invalid_returns_none(self):
        self.assertIsNone(JiraIngestionJob._parse_jira_timestamp("not-a-date"))

    # ------------------------------------------------------------------
    # Integration: process_item delegates to base with correct data
    # ------------------------------------------------------------------

    def test_process_item_calls_vector_store_and_metadata_tracker(self):
        issue = _make_issue(
            key="TEST-99", summary="Full flow", description="desc"
        )
        item = IngestionItem(
            id="jira:TEST-99",
            source_ref=issue,
            last_modified=datetime(2024, 6, 1, 0, 0, 0),
        )

        md_result = Mock()
        md_result.text_content = "desc"
        self.mock_md.convert_stream.return_value = md_result

        job = self._make_job()
        job.vector_manager.insert_documents = Mock()

        with (
            patch.object(
                job.metadata_tracker, "get_latest_record", return_value=None
            ),
            patch.object(
                job.metadata_tracker, "record_metadata"
            ) as mock_record,
            patch.object(job.metadata_tracker, "delete_previous_embeddings"),
        ):
            result = job.process_item(item)

            self.assertEqual(result, 1)
            job.vector_manager.insert_documents.assert_called_once()
            mock_record.assert_called_once()

    def test_process_item_skips_duplicate_checksum(self):
        issue = _make_issue(key="TEST-99", description="same content")
        item = IngestionItem(id="jira:TEST-99", source_ref=issue)

        md_result = Mock()
        md_result.text_content = "same content"
        self.mock_md.convert_stream.return_value = md_result

        job = self._make_job()
        job._seen_add = Mock(return_value=False)  # simulate already seen

        with patch.object(
            job.metadata_tracker, "get_latest_record", return_value=None
        ):
            with patch.object(job.metadata_tracker, "record_metadata"):
                job.vector_manager.insert_documents = Mock()
                result = job.process_item(item)

        self.assertEqual(result, 0)
        job.vector_manager.insert_documents.assert_not_called()


if __name__ == "__main__":
    unittest.main()
