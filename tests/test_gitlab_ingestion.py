import unittest
from datetime import UTC, datetime
from unittest.mock import Mock, patch

from llama_index.readers.gitlab import GitLabIssuesReader

from tasks.gitlab_ingestion import GitLabIngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(
    gitlab_url="https://gitlab.com",
    personal_token=None,
    project_id=12345,
    group_id=None,
    ref="main",
    path=None,
    file_path=None,
    recursive=True,
    include_issues=False,
    issues_state="opened",
    issues_labels="",
    issues_assignee=None,
    issues_author=None,
    issues_milestone=None,
    issues_search=None,
    issues_get_all=False,
):
    return {
        "name": "test_gitlab",
        "config": {
            "gitlab_url": gitlab_url,
            "personal_token": personal_token or "test_token",
            "project_id": project_id,
            "group_id": group_id,
            "ref": ref,
            "path": path,
            "file_path": file_path,
            "recursive": recursive,
            "include_issues": include_issues,
            "issues_state": issues_state,
            "issues_labels": issues_labels,
            "issues_assignee": issues_assignee,
            "issues_author": issues_author,
            "issues_milestone": issues_milestone,
            "issues_search": issues_search,
            "issues_get_all": issues_get_all,
        },
    }


def _make_file_doc(file_path="README.md", text="File content"):
    doc = Mock()
    doc.doc_id = "abc123"
    doc.text = text
    doc.extra_info = {"file_path": file_path, "file_name": file_path.split("/")[-1], "url": ""}
    return doc


def _make_issue_doc(iid="42", text="Issue title\nIssue body", state="opened", labels=None):
    doc = Mock()
    doc.doc_id = iid
    doc.text = text
    doc.extra_info = {
        "state": state,
        "labels": labels or [],
        "created_at": "2024-01-15T10:00:00Z",
        "closed_at": None,
        "url": f"https://gitlab.com/api/v4/projects/12345/issues/{iid}",
        "source": f"https://gitlab.com/mygroup/myrepo/-/issues/{iid}",
    }
    return doc


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class TestGitLabIngestionJob(unittest.TestCase):
    def setUp(self):
        self._gitlab_patcher = patch("tasks.gitlab_ingestion.gitlab.Gitlab")
        self._repo_reader_patcher = patch("tasks.gitlab_ingestion.GitLabRepositoryReader")
        self._issues_reader_patcher = patch("tasks.gitlab_ingestion.GitLabIssuesReader")

        self.mock_gitlab_class = self._gitlab_patcher.start()
        self.mock_repo_reader_class = self._repo_reader_patcher.start()
        self.mock_issues_reader_class = self._issues_reader_patcher.start()

        # Restore real enums on the mocked class so connector and tests use the same values
        self.mock_issues_reader_class.IssueState = GitLabIssuesReader.IssueState
        self.mock_issues_reader_class.Scope = GitLabIssuesReader.Scope
        self.mock_issues_reader_class.IssueType = GitLabIssuesReader.IssueType

        self.mock_repo_reader = Mock()
        self.mock_issues_reader = Mock()
        self.mock_repo_reader_class.return_value = self.mock_repo_reader
        self.mock_issues_reader_class.return_value = self.mock_issues_reader

    def tearDown(self):
        self._gitlab_patcher.stop()
        self._repo_reader_patcher.stop()
        self._issues_reader_patcher.stop()

    def _make_job(self, **kwargs):
        return GitLabIngestionJob(_make_config(**kwargs))

    # ------------------------------------------------------------------
    # source_type
    # ------------------------------------------------------------------

    def test_source_type(self):
        self.assertEqual(self._make_job().source_type, "gitlab")

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def test_missing_gitlab_url_raises(self):
        with self.assertRaises(ValueError):
            GitLabIngestionJob({"name": "x", "config": {"personal_token": "t", "project_id": 1}})

    def test_missing_personal_token_raises(self):
        with self.assertRaises(ValueError):
            GitLabIngestionJob({"name": "x", "config": {"gitlab_url": "https://gitlab.com", "project_id": 1}})

    def test_missing_project_and_group_raises(self):
        with self.assertRaises(ValueError):
            GitLabIngestionJob(
                {
                    "name": "x",
                    "config": {
                        "gitlab_url": "https://gitlab.com",
                        "personal_token": "t",
                    },
                }
            )

    def test_group_id_only_with_issues_is_valid(self):
        job = GitLabIngestionJob(
            {
                "name": "x",
                "config": {
                    "gitlab_url": "https://gitlab.com",
                    "personal_token": "t",
                    "group_id": 999,
                    "include_issues": True,
                },
            }
        )
        self.assertIsNone(job.project_id)
        self.assertEqual(job.group_id, 999)

    # ------------------------------------------------------------------
    # list_items — files
    # ------------------------------------------------------------------

    def test_list_items_yields_file_items(self):
        self.mock_repo_reader.load_data.return_value = [
            _make_file_doc("README.md"),
            _make_file_doc("src/main.py"),
        ]
        job = self._make_job()
        items = list(job.list_items())
        self.assertEqual(len(items), 2)
        self.assertIn(":file:", items[0].id)

    def test_list_items_file_id_format(self):
        self.mock_repo_reader.load_data.return_value = [_make_file_doc("docs/guide.md")]
        job = self._make_job(project_id=12345)
        items = list(job.list_items())
        self.assertEqual(items[0].id, "gitlab:12345:file:docs/guide.md")

    def test_list_items_passes_ref_to_reader(self):
        self.mock_repo_reader.load_data.return_value = []
        job = self._make_job(ref="develop")
        list(job.list_items())
        call_kwargs = self.mock_repo_reader.load_data.call_args.kwargs
        self.assertEqual(call_kwargs["ref"], "develop")

    def test_list_items_passes_path_and_recursive(self):
        self.mock_repo_reader.load_data.return_value = []
        job = self._make_job(path="docs", recursive=False)
        list(job.list_items())
        call_kwargs = self.mock_repo_reader.load_data.call_args.kwargs
        self.assertEqual(call_kwargs["path"], "docs")
        self.assertFalse(call_kwargs["recursive"])

    def test_list_items_file_error_raises(self):
        self.mock_repo_reader.load_data.side_effect = Exception("API error")
        job = self._make_job()
        with self.assertRaises(Exception):
            list(job.list_items())

    # ------------------------------------------------------------------
    # list_items — issues
    # ------------------------------------------------------------------

    def test_list_items_no_issues_when_disabled(self):
        self.mock_repo_reader.load_data.return_value = []
        job = self._make_job(include_issues=False)
        list(job.list_items())
        self.mock_issues_reader.load_data.assert_not_called()

    def test_list_items_yields_issue_items(self):
        self.mock_repo_reader.load_data.return_value = []
        self.mock_issues_reader.load_data.return_value = [
            _make_issue_doc("1"),
            _make_issue_doc("2"),
        ]
        job = self._make_job(include_issues=True)
        items = list(job.list_items())
        self.assertEqual(len(items), 2)
        self.assertIn(":issue:", items[0].id)

    def test_list_items_issue_id_format(self):
        self.mock_repo_reader.load_data.return_value = []
        self.mock_issues_reader.load_data.return_value = [_make_issue_doc("7")]
        job = self._make_job(project_id=12345, include_issues=True)
        items = list(job.list_items())
        self.assertEqual(items[0].id, "gitlab:12345:issue:7")

    def test_list_items_issue_state_passed(self):
        self.mock_repo_reader.load_data.return_value = []
        self.mock_issues_reader.load_data.return_value = []
        job = self._make_job(include_issues=True, issues_state="closed")
        list(job.list_items())
        call_kwargs = self.mock_issues_reader.load_data.call_args.kwargs
        self.assertEqual(call_kwargs["state"], GitLabIssuesReader.IssueState.CLOSED)

    def test_list_items_issue_labels_passed(self):
        self.mock_repo_reader.load_data.return_value = []
        self.mock_issues_reader.load_data.return_value = []
        job = self._make_job(include_issues=True, issues_labels="bug,docs")
        list(job.list_items())
        call_kwargs = self.mock_issues_reader.load_data.call_args.kwargs
        self.assertIn("bug", call_kwargs["labels"])
        self.assertIn("docs", call_kwargs["labels"])

    def test_list_items_issue_error_raises(self):
        self.mock_repo_reader.load_data.return_value = []
        self.mock_issues_reader.load_data.side_effect = Exception("403 Forbidden")
        job = self._make_job(include_issues=True)
        with self.assertRaises(Exception):
            list(job.list_items())

    # ------------------------------------------------------------------
    # get_raw_content
    # ------------------------------------------------------------------

    def test_get_raw_content_returns_doc_text(self):
        doc = _make_file_doc(text="# Hello")
        item = IngestionItem(id="gitlab:1:file:README.md", source_ref=doc)
        job = self._make_job()
        self.assertEqual(job.get_raw_content(item), "# Hello")

    def test_get_raw_content_none_returns_empty(self):
        doc = _make_file_doc(text=None)
        item = IngestionItem(id="gitlab:1:file:README.md", source_ref=doc)
        job = self._make_job()
        self.assertEqual(job.get_raw_content(item), "")

    # ------------------------------------------------------------------
    # get_item_name
    # ------------------------------------------------------------------

    def test_get_item_name_file(self):
        doc = _make_file_doc(file_path="docs/guide.md")
        item = IngestionItem(id="gitlab:1:file:docs/guide.md", source_ref=doc)
        job = self._make_job()
        self.assertEqual(job.get_item_name(item), "docs_guide.md")

    def test_get_item_name_issue(self):
        doc = _make_issue_doc(iid="42")
        item = IngestionItem(id="gitlab:12345:issue:42", source_ref=doc)
        job = self._make_job(project_id=12345)
        self.assertEqual(job.get_item_name(item), "gitlab_issue_12345_42")

    def test_get_item_name_file_no_doc_id_or_file_path_falls_back_to_item_id(self):
        doc = Mock()
        doc.doc_id = None
        doc.extra_info = {}
        item = IngestionItem(id="gitlab:1:file:fallback", source_ref=doc)
        job = self._make_job()
        self.assertEqual(job.get_item_name(item), "gitlab:1:file:fallback")

    def test_get_item_name_truncates_to_255(self):
        doc = _make_file_doc(file_path="a/" * 200 + "file.md")
        item = IngestionItem(id="gitlab:1:file:x", source_ref=doc)
        job = self._make_job()
        self.assertLessEqual(len(job.get_item_name(item)), 255)

    # ------------------------------------------------------------------
    # get_extra_metadata — files
    # ------------------------------------------------------------------

    def test_get_extra_metadata_file(self):
        doc = _make_file_doc(file_path="README.md")
        item = IngestionItem(id="gitlab:12345:file:README.md", source_ref=doc)
        job = self._make_job(project_id=12345)
        meta = job.get_extra_metadata(item, "", {})
        self.assertEqual(meta["item_type"], "file")
        self.assertEqual(meta["file_path"], "README.md")
        self.assertEqual(meta["gitlab_url"], "https://gitlab.com")

    # ------------------------------------------------------------------
    # get_extra_metadata — issues
    # ------------------------------------------------------------------

    def test_get_extra_metadata_issue(self):
        doc = _make_issue_doc(iid="7", state="opened", labels=["bug"])
        item = IngestionItem(id="gitlab:12345:issue:7", source_ref=doc)
        job = self._make_job(project_id=12345, include_issues=True)
        meta = job.get_extra_metadata(item, "", {})
        self.assertEqual(meta["item_type"], "issue")
        self.assertEqual(meta["issue_number"], "7")
        self.assertEqual(meta["state"], "opened")
        self.assertIn("bug", meta["labels"])
        self.assertIn("gitlab.com", meta["url"])

    def test_get_extra_metadata_issue_assignee_present(self):
        doc = _make_issue_doc(iid="8")
        doc.extra_info["assignee"] = "octocat"
        item = IngestionItem(id="gitlab:12345:issue:8", source_ref=doc)
        job = self._make_job(project_id=12345, include_issues=True)
        meta = job.get_extra_metadata(item, "", {})
        self.assertEqual(meta["assignee"], "octocat")

    def test_get_extra_metadata_issue_no_assignee(self):
        doc = _make_issue_doc(iid="9")
        item = IngestionItem(id="gitlab:12345:issue:9", source_ref=doc)
        job = self._make_job(project_id=12345, include_issues=True)
        meta = job.get_extra_metadata(item, "", {})
        self.assertNotIn("assignee", meta)

    # ------------------------------------------------------------------
    # _parse_list
    # ------------------------------------------------------------------

    def test_parse_list_comma_string(self):
        self.assertEqual(GitLabIngestionJob._parse_list("bug, docs, wip"), ["bug", "docs", "wip"])

    def test_parse_list_list_input(self):
        self.assertEqual(GitLabIngestionJob._parse_list(["bug", "docs"]), ["bug", "docs"])

    def test_parse_list_empty(self):
        self.assertIsNone(GitLabIngestionJob._parse_list(""))
        self.assertIsNone(GitLabIngestionJob._parse_list(None))

    def test_parse_list_non_string_elements(self):
        self.assertEqual(GitLabIngestionJob._parse_list([1, 2, 3]), ["1", "2", "3"])

    def test_parse_list_all_blank_list_returns_none(self):
        self.assertIsNone(GitLabIngestionJob._parse_list(["", "  ", ""]))

    def test_resolve_state_opened(self):
        self.assertEqual(
            GitLabIngestionJob._resolve_state_enum("opened"),
            GitLabIssuesReader.IssueState.OPEN,
        )

    def test_resolve_state_closed(self):
        self.assertEqual(
            GitLabIngestionJob._resolve_state_enum("closed"),
            GitLabIssuesReader.IssueState.CLOSED,
        )

    def test_resolve_state_all(self):
        self.assertEqual(
            GitLabIngestionJob._resolve_state_enum("all"),
            GitLabIssuesReader.IssueState.ALL,
        )

    def test_resolve_state_unknown_defaults_to_open(self):
        self.assertEqual(
            GitLabIngestionJob._resolve_state_enum("bogus"),
            GitLabIssuesReader.IssueState.OPEN,
        )

    # ------------------------------------------------------------------
    # _resolve_scope_enum
    # ------------------------------------------------------------------

    def test_resolve_scope_created_by_me(self):
        self.assertEqual(
            GitLabIngestionJob._resolve_scope_enum("created_by_me"),
            GitLabIssuesReader.Scope.CREATED_BY_ME,
        )

    def test_resolve_scope_assigned_to_me(self):
        self.assertEqual(
            GitLabIngestionJob._resolve_scope_enum("assigned_to_me"),
            GitLabIssuesReader.Scope.ASSIGNED_TO_ME,
        )

    def test_resolve_scope_all(self):
        self.assertEqual(
            GitLabIngestionJob._resolve_scope_enum("all"),
            GitLabIssuesReader.Scope.ALL,
        )

    def test_resolve_scope_none_returns_none(self):
        self.assertIsNone(GitLabIngestionJob._resolve_scope_enum(None))

    def test_resolve_scope_unknown_returns_none(self):
        self.assertIsNone(GitLabIngestionJob._resolve_scope_enum("bogus"))

    # ------------------------------------------------------------------
    # _resolve_issue_type_enum
    # ------------------------------------------------------------------

    def test_resolve_issue_type_issue(self):
        self.assertEqual(
            GitLabIngestionJob._resolve_issue_type_enum("issue"),
            GitLabIssuesReader.IssueType.ISSUE,
        )

    def test_resolve_issue_type_incident(self):
        self.assertEqual(
            GitLabIngestionJob._resolve_issue_type_enum("incident"),
            GitLabIssuesReader.IssueType.INCIDENT,
        )

    def test_resolve_issue_type_none_returns_none(self):
        self.assertIsNone(GitLabIngestionJob._resolve_issue_type_enum(None))

    def test_resolve_issue_type_unknown_returns_none(self):
        self.assertIsNone(GitLabIngestionJob._resolve_issue_type_enum("bogus"))

    # ------------------------------------------------------------------
    # _parse_timestamp
    # ------------------------------------------------------------------

    def test_parse_timestamp_valid(self):
        result = GitLabIngestionJob._parse_timestamp("2024-01-15T10:00:00Z")
        self.assertIsNotNone(result)
        self.assertEqual(result.year, 2024)

    def test_parse_timestamp_none(self):
        self.assertIsNone(GitLabIngestionJob._parse_timestamp(None))

    def test_parse_timestamp_invalid(self):
        self.assertIsNone(GitLabIngestionJob._parse_timestamp("not-a-date"))

    # ------------------------------------------------------------------
    # file_path and new issue filters passed to readers
    # ------------------------------------------------------------------

    def test_list_items_passes_file_path(self):
        self.mock_repo_reader.load_data.return_value = []
        job = self._make_job(file_path="README.md")
        list(job.list_items())
        call_kwargs = self.mock_repo_reader.load_data.call_args.kwargs
        self.assertEqual(call_kwargs["file_path"], "README.md")

    def test_list_items_issue_date_filters_passed(self):
        self.mock_repo_reader.load_data.return_value = []
        self.mock_issues_reader.load_data.return_value = []
        job = self._make_job(include_issues=True)
        job.issues_created_after = datetime(2024, 1, 1, tzinfo=UTC)
        job.issues_created_before = datetime(2024, 12, 31, tzinfo=UTC)
        list(job.list_items())
        call_kwargs = self.mock_issues_reader.load_data.call_args.kwargs
        self.assertEqual(call_kwargs["created_after"].year, 2024)
        self.assertEqual(call_kwargs["created_before"].year, 2024)

    def test_list_items_issue_scope_passed(self):
        self.mock_repo_reader.load_data.return_value = []
        self.mock_issues_reader.load_data.return_value = []
        job = self._make_job(include_issues=True)
        job.issues_scope = GitLabIssuesReader.Scope.CREATED_BY_ME
        list(job.list_items())
        call_kwargs = self.mock_issues_reader.load_data.call_args.kwargs
        self.assertEqual(call_kwargs["scope"], GitLabIssuesReader.Scope.CREATED_BY_ME)

    def test_list_items_issue_iids_passed(self):
        self.mock_repo_reader.load_data.return_value = []
        self.mock_issues_reader.load_data.return_value = []
        job = self._make_job(include_issues=True)
        job.issues_iids = [1, 2, 3]
        list(job.list_items())
        call_kwargs = self.mock_issues_reader.load_data.call_args.kwargs
        self.assertEqual(call_kwargs["iids"], [1, 2, 3])

    def test_list_items_issue_confidential_passed(self):
        self.mock_repo_reader.load_data.return_value = []
        self.mock_issues_reader.load_data.return_value = []
        job = self._make_job(include_issues=True)
        job.issues_confidential = True
        list(job.list_items())
        call_kwargs = self.mock_issues_reader.load_data.call_args.kwargs
        self.assertTrue(call_kwargs["confidential"])

    # ------------------------------------------------------------------
    # _parse_bool
    # ------------------------------------------------------------------

    def test_parse_bool_true_values(self):
        for v in [True, "true", "True", "1", "yes", "on"]:
            self.assertTrue(GitLabIngestionJob._parse_bool(v), msg=f"expected True for {v!r}")

    def test_parse_bool_false_values(self):
        for v in [False, "false", "False", "0", "no", "off"]:
            self.assertFalse(GitLabIngestionJob._parse_bool(v), msg=f"expected False for {v!r}")

    def test_parse_bool_none_uses_default(self):
        self.assertTrue(GitLabIngestionJob._parse_bool(None, default=True))
        self.assertFalse(GitLabIngestionJob._parse_bool(None, default=False))

    def test_parse_bool_flows_through_recursive(self):
        job = self._make_job(recursive="false")
        self.assertFalse(job.recursive)

    def test_parse_bool_flows_through_include_issues(self):
        job = self._make_job(include_issues="false")
        self.assertFalse(job.include_issues)

    def test_parse_bool_flows_through_issues_get_all(self):
        job = self._make_job(issues_get_all="true")
        self.assertTrue(job.issues_get_all)

    # ------------------------------------------------------------------
    # _parse_bool_optional
    # ------------------------------------------------------------------

    def test_parse_bool_optional_none_returns_none(self):
        self.assertIsNone(GitLabIngestionJob._parse_bool_optional(None))

    def test_parse_bool_optional_true_values(self):
        for v in [True, "true", "1", "yes", "on"]:
            self.assertTrue(GitLabIngestionJob._parse_bool_optional(v), msg=f"expected True for {v!r}")

    def test_parse_bool_optional_false_values(self):
        for v in [False, "false", "0", "no", "off"]:
            self.assertFalse(GitLabIngestionJob._parse_bool_optional(v), msg=f"expected False for {v!r}")

    def test_parse_bool_optional_flows_through_confidential(self):
        job = self._make_job(include_issues=True)
        job.issues_confidential = GitLabIngestionJob._parse_bool_optional("true")
        self.assertTrue(job.issues_confidential)

    # ------------------------------------------------------------------
    # Fail-fast: group_id only + include_issues=False raises ValueError
    # ------------------------------------------------------------------

    def test_group_id_only_no_issues_raises(self):
        with self.assertRaises(ValueError):
            GitLabIngestionJob(
                {
                    "name": "x",
                    "config": {
                        "gitlab_url": "https://gitlab.com",
                        "personal_token": "test_token",
                        "group_id": 999,
                        "include_issues": False,
                    },
                }
            )

    # ------------------------------------------------------------------
    # issue last_modified prefers updated_at over created_at
    # ------------------------------------------------------------------

    def test_list_items_issue_last_modified_uses_created_at(self):
        doc = _make_issue_doc("5")
        doc.extra_info["created_at"] = "2024-03-10T08:00:00Z"
        self.mock_repo_reader.load_data.return_value = []
        self.mock_issues_reader.load_data.return_value = [doc]
        job = self._make_job(include_issues=True)
        items = list(job.list_items())
        self.assertEqual(items[0].last_modified.year, 2024)


if __name__ == "__main__":
    unittest.main()
