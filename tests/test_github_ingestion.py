import unittest
from unittest.mock import Mock, patch

from llama_index.readers.github import GithubRepositoryReader, GitHubRepositoryIssuesReader
from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.github_ingestion import GitHubIngestionJob


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(
    owner="myorg",
    repo="myrepo",
    branch="main",
    commit_sha="",
    personal_token="ghp_test_token",
    github_app_id="",
    github_app_installation_id="",
    github_app_private_key="",
    include_extensions="",
    exclude_extensions="",
    include_directories="",
    exclude_directories="",
    include_issues=False,
    include_issues_labels="",
    exclude_issues_labels="",
):
    return {
        "name": "test_github",
        "config": {
            "owner": owner,
            "repo": repo,
            "branch": branch,
            "commit_sha": commit_sha,
            "personal_token": personal_token,
            "github_app_id": github_app_id,
            "github_app_installation_id": github_app_installation_id,
            "github_app_private_key": github_app_private_key,
            "include_extensions": include_extensions,
            "exclude_extensions": exclude_extensions,
            "include_directories": include_directories,
            "exclude_directories": exclude_directories,
            "include_issues": include_issues,
            "include_issues_labels": include_issues_labels,
            "exclude_issues_labels": exclude_issues_labels,
        },
    }


def _make_file_doc(file_path="README.md", text="File content"):
    doc = Mock()
    doc.doc_id = file_path
    doc.text = text
    file_name = file_path.split("/")[-1]
    doc.metadata = {
        "file_path": file_path,
        "file_name": file_name,
        "url": f"https://github.com/myorg/myrepo/blob/main/{file_path}",
    }
    return doc


def _make_issue_doc(number="42", text="Issue title\nIssue body", state="open", labels=None):
    doc = Mock()
    doc.doc_id = number
    doc.text = text
    doc.metadata = {
        "state": state,
        "url": f"https://api.github.com/repos/myorg/myrepo/issues/{number}",
        "source": f"https://github.com/myorg/myrepo/issues/{number}",
        "labels": labels or [],
    }
    return doc


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class TestGitHubIngestionJob(unittest.TestCase):
    def setUp(self):
        self.github_client_patcher = patch("tasks.github_ingestion.GithubClient")
        self.issues_client_patcher = patch("tasks.github_ingestion.GitHubIssuesClient")

        self.mock_github_client_class = self.github_client_patcher.start()
        self.mock_issues_client_class = self.issues_client_patcher.start()

        # Patch only the instances returned by the readers, keeping real FilterType enums
        self.mock_repo_reader = Mock()
        self.mock_issues_reader = Mock()

        self._repo_reader_patcher = patch.object(
            GithubRepositoryReader, "__init__", return_value=None
        )
        self._issues_reader_patcher = patch.object(
            GitHubRepositoryIssuesReader, "__init__", return_value=None
        )
        self._repo_reader_patcher.start()
        self._issues_reader_patcher.start()

        # Patch the constructors to return our mocks
        self._repo_new_patcher = patch(
            "tasks.github_ingestion.GithubRepositoryReader", return_value=self.mock_repo_reader
        )
        self._issues_new_patcher = patch(
            "tasks.github_ingestion.GitHubRepositoryIssuesReader",
            return_value=self.mock_issues_reader,
        )
        self.mock_repo_reader_class = self._repo_new_patcher.start()
        self.mock_issues_reader_class = self._issues_new_patcher.start()

        # Restore real FilterType on the mock class
        self.mock_repo_reader_class.FilterType = GithubRepositoryReader.FilterType
        self.mock_issues_reader_class.FilterType = GitHubRepositoryIssuesReader.FilterType

    def tearDown(self):
        self._repo_new_patcher.stop()
        self._issues_new_patcher.stop()
        self._repo_reader_patcher.stop()
        self._issues_reader_patcher.stop()
        self.github_client_patcher.stop()
        self.issues_client_patcher.stop()

    def _make_job(self, **kwargs):
        return GitHubIngestionJob(_make_config(**kwargs))

    # ------------------------------------------------------------------
    # source_type
    # ------------------------------------------------------------------

    def test_source_type(self):
        self.assertEqual(self._make_job().source_type, "github")

    # ------------------------------------------------------------------
    # Validation — auth
    # ------------------------------------------------------------------

    def test_missing_auth_raises(self):
        with self.assertRaises(ValueError):
            GitHubIngestionJob({"name": "x", "config": {"owner": "o", "repo": "r"}})

    def test_pat_and_app_mutually_exclusive(self):
        with self.assertRaises(ValueError):
            self._make_job(
                personal_token="ghp_token",
                github_app_id="123",
                github_app_installation_id="456",
                github_app_private_key="key",
            )

    def test_incomplete_app_credentials_raises(self):
        with self.assertRaises(ValueError):
            self._make_job(personal_token="", github_app_id="123")

    # ------------------------------------------------------------------
    # Validation — repo params
    # ------------------------------------------------------------------

    def test_missing_owner_raises(self):
        with self.assertRaises(ValueError):
            GitHubIngestionJob(
                {"name": "x", "config": {"owner": "", "repo": "r", "personal_token": "t"}}
            )

    def test_missing_repo_raises(self):
        with self.assertRaises(ValueError):
            GitHubIngestionJob(
                {"name": "x", "config": {"owner": "o", "repo": "", "personal_token": "t"}}
            )

    def test_branch_and_commit_sha_mutually_exclusive(self):
        with self.assertRaises(ValueError):
            self._make_job(branch="main", commit_sha="abc123")

    def test_defaults_to_main_when_no_branch_or_commit(self):
        job = GitHubIngestionJob(
            {"name": "x", "config": {"owner": "o", "repo": "r", "personal_token": "t"}}
        )
        self.assertEqual(job.branch, "main")
        self.assertIsNone(job.commit_sha)

    # ------------------------------------------------------------------
    # Validation — filter mutual exclusivity
    # ------------------------------------------------------------------

    def test_include_and_exclude_extensions_mutually_exclusive(self):
        with self.assertRaises(ValueError):
            self._make_job(include_extensions="md", exclude_extensions="py")

    def test_include_and_exclude_directories_mutually_exclusive(self):
        with self.assertRaises(ValueError):
            self._make_job(include_directories="docs", exclude_directories="tests")

    def test_include_and_exclude_issues_labels_mutually_exclusive(self):
        with self.assertRaises(ValueError):
            self._make_job(
                include_issues=True,
                include_issues_labels="bug",
                exclude_issues_labels="wontfix",
            )

    # ------------------------------------------------------------------
    # Reader initialization
    # ------------------------------------------------------------------

    def test_repo_reader_initialized_with_owner_and_repo(self):
        self._make_job(owner="acme", repo="rocket")
        call_kwargs = self.mock_repo_reader_class.call_args.kwargs
        self.assertEqual(call_kwargs["owner"], "acme")
        self.assertEqual(call_kwargs["repo"], "rocket")

    def test_include_extensions_passed_as_include_filter(self):
        self._make_job(include_extensions="md,py")
        call_kwargs = self.mock_repo_reader_class.call_args.kwargs
        exts, ftype = call_kwargs["filter_file_extensions"]
        self.assertIn("md", exts)
        self.assertIn("py", exts)
        self.assertEqual(ftype, GithubRepositoryReader.FilterType.INCLUDE)

    def test_exclude_extensions_passed_as_exclude_filter(self):
        self._make_job(exclude_extensions="png,jpg")
        call_kwargs = self.mock_repo_reader_class.call_args.kwargs
        exts, ftype = call_kwargs["filter_file_extensions"]
        self.assertIn("png", exts)
        self.assertEqual(ftype, GithubRepositoryReader.FilterType.EXCLUDE)

    def test_include_directories_passed_as_include_filter(self):
        self._make_job(include_directories="docs,src")
        call_kwargs = self.mock_repo_reader_class.call_args.kwargs
        dirs, ftype = call_kwargs["filter_directories"]
        self.assertIn("docs", dirs)
        self.assertEqual(ftype, GithubRepositoryReader.FilterType.INCLUDE)

    def test_exclude_directories_passed_as_exclude_filter(self):
        self._make_job(exclude_directories="tests")
        call_kwargs = self.mock_repo_reader_class.call_args.kwargs
        dirs, ftype = call_kwargs["filter_directories"]
        self.assertIn("tests", dirs)
        self.assertEqual(ftype, GithubRepositoryReader.FilterType.EXCLUDE)

    def test_no_filters_passes_none(self):
        self._make_job()
        call_kwargs = self.mock_repo_reader_class.call_args.kwargs
        self.assertIsNone(call_kwargs["filter_file_extensions"])
        self.assertIsNone(call_kwargs["filter_directories"])

    # ------------------------------------------------------------------
    # list_items — files
    # ------------------------------------------------------------------

    def test_list_items_yields_file_items_by_branch(self):
        self.mock_repo_reader.load_data.return_value = [
            _make_file_doc("README.md"),
            _make_file_doc("src/main.py"),
        ]
        job = self._make_job(branch="main")
        items = list(job.list_items())

        self.assertEqual(len(items), 2)
        self.mock_repo_reader.load_data.assert_called_once_with(branch="main")
        self.assertIn("README.md", items[0].id)

    def test_list_items_uses_commit_sha_when_set(self):
        self.mock_repo_reader.load_data.return_value = [_make_file_doc()]
        job = self._make_job(branch="", commit_sha="abc123")
        list(job.list_items())
        self.mock_repo_reader.load_data.assert_called_once_with(commit_sha="abc123")

    def test_list_items_repo_error_yields_nothing(self):
        self.mock_repo_reader.load_data.side_effect = Exception("API error")
        job = self._make_job()
        items = list(job.list_items())
        self.assertEqual(items, [])

    def test_list_items_file_item_id_format(self):
        self.mock_repo_reader.load_data.return_value = [_make_file_doc("docs/guide.md")]
        job = self._make_job(owner="acme", repo="proj")
        items = list(job.list_items())
        self.assertEqual(items[0].id, "github:acme/proj:docs/guide.md")

    # ------------------------------------------------------------------
    # list_items — issues
    # ------------------------------------------------------------------

    def test_list_items_includes_issues_when_enabled(self):
        self.mock_repo_reader.load_data.return_value = []
        self.mock_issues_reader.load_data.return_value = [
            _make_issue_doc("1"),
            _make_issue_doc("2"),
        ]
        job = self._make_job(include_issues=True)
        items = list(job.list_items())

        self.assertEqual(len(items), 2)
        self.assertIn(":issue:", items[0].id)

    def test_list_items_skips_pull_requests(self):
        pr_doc = _make_issue_doc("10")
        pr_doc.metadata["source"] = "https://github.com/myorg/myrepo/pull/10"
        self.mock_repo_reader.load_data.return_value = []
        self.mock_issues_reader.load_data.return_value = [
            _make_issue_doc("1"),
            pr_doc,
            _make_issue_doc("2"),
        ]
        job = self._make_job(include_issues=True)
        items = list(job.list_items())
        self.assertEqual(len(items), 2)
        self.assertNotIn("issue:10", " ".join(i.id for i in items))

    def test_list_items_no_issues_when_disabled(self):
        self.mock_repo_reader.load_data.return_value = []
        job = self._make_job(include_issues=False)
        list(job.list_items())
        self.mock_issues_reader.load_data.assert_not_called()

    def test_list_items_issues_error_yields_nothing(self):
        self.mock_repo_reader.load_data.return_value = []
        self.mock_issues_reader.load_data.side_effect = Exception("403 Forbidden")
        job = self._make_job(include_issues=True)
        items = list(job.list_items())
        self.assertEqual(items, [])

    def test_list_items_include_labels_passed_to_reader(self):
        self.mock_repo_reader.load_data.return_value = []
        self.mock_issues_reader.load_data.return_value = []
        job = self._make_job(include_issues=True, include_issues_labels="bug,docs")
        list(job.list_items())

        call_kwargs = self.mock_issues_reader.load_data.call_args.kwargs
        label_filters = call_kwargs["labelFilters"]
        self.assertIsNotNone(label_filters)
        labels = [lf[0] for lf in label_filters]
        self.assertIn("bug", labels)
        self.assertIn("docs", labels)
        for _, ftype in label_filters:
            self.assertEqual(ftype, GitHubRepositoryIssuesReader.FilterType.INCLUDE)

    def test_list_items_exclude_labels_passed_to_reader(self):
        self.mock_repo_reader.load_data.return_value = []
        self.mock_issues_reader.load_data.return_value = []
        job = self._make_job(include_issues=True, exclude_issues_labels="wontfix")
        list(job.list_items())

        call_kwargs = self.mock_issues_reader.load_data.call_args.kwargs
        label_filters = call_kwargs["labelFilters"]
        self.assertEqual(label_filters[0][1], GitHubRepositoryIssuesReader.FilterType.EXCLUDE)

    def test_list_items_no_label_filter_passes_none(self):
        self.mock_repo_reader.load_data.return_value = []
        self.mock_issues_reader.load_data.return_value = []
        job = self._make_job(include_issues=True)
        list(job.list_items())
        call_kwargs = self.mock_issues_reader.load_data.call_args.kwargs
        self.assertIsNone(call_kwargs["labelFilters"])

    # ------------------------------------------------------------------
    # get_raw_content
    # ------------------------------------------------------------------

    def test_get_raw_content_returns_doc_text(self):
        doc = _make_file_doc(text="# Hello\nWorld")
        item = IngestionItem(id="github:o/r:README.md", source_ref=doc)
        job = self._make_job()
        self.assertEqual(job.get_raw_content(item), "# Hello\nWorld")

    def test_get_raw_content_none_text_returns_empty(self):
        doc = _make_file_doc(text=None)
        item = IngestionItem(id="github:o/r:README.md", source_ref=doc)
        job = self._make_job()
        self.assertEqual(job.get_raw_content(item), "")

    # ------------------------------------------------------------------
    # get_item_name
    # ------------------------------------------------------------------

    def test_get_item_name_returns_file_path(self):
        doc = _make_file_doc(file_path="docs/guide.md")
        item = IngestionItem(id="github:o/r:docs/guide.md", source_ref=doc)
        job = self._make_job()
        self.assertEqual(job.get_item_name(item), "docs/guide.md")

    def test_get_item_name_sanitizes_special_chars(self):
        doc = _make_file_doc(file_path="path with spaces!.md")
        item = IngestionItem(id="github:o/r:x", source_ref=doc)
        job = self._make_job()
        name = job.get_item_name(item)
        self.assertNotIn(" ", name)
        self.assertNotIn("!", name)

    def test_get_item_name_truncates_to_255(self):
        doc = _make_file_doc(file_path="a/" * 200 + "file.md")
        item = IngestionItem(id="github:o/r:x", source_ref=doc)
        job = self._make_job()
        self.assertLessEqual(len(job.get_item_name(item)), 255)

    # ------------------------------------------------------------------
    # get_document_metadata — files
    # ------------------------------------------------------------------

    def test_get_document_metadata_file_has_required_fields(self):
        doc = _make_file_doc(file_path="README.md")
        item = IngestionItem(id="github:myorg/myrepo:README.md", source_ref=doc)
        job = self._make_job(owner="myorg", repo="myrepo", branch="main")
        metadata = job.get_document_metadata(
            item=item, item_name="README.md", checksum="abc", version=1, last_modified=None
        )
        self.assertEqual(metadata["source"], "github")
        self.assertEqual(metadata["owner"], "myorg")
        self.assertEqual(metadata["repo"], "myrepo")
        self.assertEqual(metadata["item_type"], "file")
        self.assertEqual(metadata["file_path"], "README.md")
        self.assertIn("github.com/myorg/myrepo/blob/main/README.md", metadata["url"])
        self.assertEqual(metadata["file_name"], "README.md")

    # ------------------------------------------------------------------
    # get_document_metadata — issues
    # ------------------------------------------------------------------

    def test_get_document_metadata_issue_has_required_fields(self):
        doc = _make_issue_doc(number="42", state="open", labels=["bug"])
        item = IngestionItem(id="github:myorg/myrepo:issue:42", source_ref=doc)
        job = self._make_job(owner="myorg", repo="myrepo", include_issues=True)
        metadata = job.get_document_metadata(
            item=item, item_name="42", checksum="xyz", version=1, last_modified=None
        )
        self.assertEqual(metadata["item_type"], "issue")
        self.assertEqual(metadata["issue_number"], "42")
        self.assertEqual(metadata["state"], "open")
        self.assertEqual(metadata["labels"], ["bug"])
        self.assertIn("github.com/myorg/myrepo/issues/42", metadata["url"])

    def test_get_document_metadata_issue_optional_fields_included_when_present_omitted_when_absent(self):
        doc = _make_issue_doc(number="7", state="closed")
        doc.metadata["assignee"] = "octocat"
        doc.metadata["closed_at"] = "2024-01-15T10:00:00Z"
        item = IngestionItem(id="github:myorg/myrepo:issue:7", source_ref=doc)
        job = self._make_job(owner="myorg", repo="myrepo", include_issues=True)
        metadata = job.get_document_metadata(
            item=item, item_name="7", checksum="xyz", version=1, last_modified=None
        )
        self.assertEqual(metadata["assignee"], "octocat")
        self.assertEqual(metadata["closed_at"], "2024-01-15T10:00:00Z")
        self.assertEqual(metadata["state"], "closed")

        # When fields are absent they must not appear in metadata
        doc2 = _make_issue_doc(number="8")
        for field in ("state", "labels", "assignee", "closed_at"):
            doc2.metadata.pop(field, None)
        item2 = IngestionItem(id="github:myorg/myrepo:issue:8", source_ref=doc2)
        metadata2 = job.get_document_metadata(
            item=item2, item_name="8", checksum="xyz", version=1, last_modified=None
        )
        for field in ("state", "labels", "assignee", "closed_at"):
            self.assertNotIn(field, metadata2)

    # ------------------------------------------------------------------
    # _parse_list
    # ------------------------------------------------------------------

    def test_parse_list(self):
        self.assertEqual(GitHubIngestionJob._parse_list("md,py, txt"), ["md", "py", "txt"])
        self.assertEqual(GitHubIngestionJob._parse_list(["md", "py"]), ["md", "py"])
        self.assertEqual(GitHubIngestionJob._parse_list(""), [])
        self.assertEqual(GitHubIngestionJob._parse_list(None), [])
        self.assertEqual(GitHubIngestionJob._parse_list("md,,  ,py"), ["md", "py"])

    # ------------------------------------------------------------------
    # Integration: process_item
    # ------------------------------------------------------------------

    def test_process_item_calls_vector_store(self):
        self.mock_repo_reader.load_data.return_value = [_make_file_doc(text="content")]
        job = self._make_job()
        doc = _make_file_doc(text="file content")
        item = IngestionItem(id="github:o/r:README.md", source_ref=doc)
        job.vector_manager.insert_documents = Mock()

        with (
            patch.object(job.metadata_tracker, "get_latest_record", return_value=None),
            patch.object(job.metadata_tracker, "record_metadata") as mock_record,
            patch.object(job.metadata_tracker, "delete_previous_embeddings"),
        ):
            result = job.process_item(item)
            self.assertEqual(result, 1)
            job.vector_manager.insert_documents.assert_called_once()
            mock_record.assert_called_once()


if __name__ == "__main__":
    unittest.main()
