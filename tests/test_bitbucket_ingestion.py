import unittest
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import requests

from tasks.bitbucket_ingestion import BitbucketClient, BitbucketIngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem


def _api_response(values, next_url=None):
    resp = Mock()
    resp.raise_for_status = Mock()
    body = {"values": values}
    if next_url:
        body["next"] = next_url
    resp.json.return_value = body
    return resp


class TestBitbucketClient(unittest.TestCase):
    def setUp(self):
        self.get_patcher = patch("tasks.bitbucket_ingestion.requests.get")
        self.mock_get = self.get_patcher.start()
        self.client = BitbucketClient("user", "token")

    def tearDown(self):
        self.get_patcher.stop()

    def test_list_files_yields_commit_file_entries(self):
        self.mock_get.return_value = _api_response(
            [
                {"type": "commit_file", "path": "README.md"},
                {"type": "commit_file", "path": "src/main.py"},
            ]
        )
        entries = list(self.client.list_files("ws", "repo", "main"))
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]["path"], "README.md")

    def test_list_files_recurses_into_subdirectories(self):
        root = _api_response(
            [
                {"type": "commit_file", "path": "README.md"},
                {"type": "commit_directory", "path": "docs"},
            ]
        )
        docs = _api_response(
            [
                {"type": "commit_file", "path": "docs/guide.md"},
            ]
        )
        self.mock_get.side_effect = [root, docs]

        entries = list(self.client.list_files("ws", "repo", "main"))

        self.assertEqual(len(entries), 2)
        paths = {e["path"] for e in entries}
        self.assertIn("README.md", paths)
        self.assertIn("docs/guide.md", paths)
        self.assertEqual(self.mock_get.call_count, 2)

    def test_list_files_paginates_via_next_url(self):
        page1 = _api_response(
            [{"type": "commit_file", "path": "a.md"}],
            next_url="https://api.bitbucket.org/2.0/page2",
        )
        page2 = _api_response([{"type": "commit_file", "path": "b.md"}])
        self.mock_get.side_effect = [page1, page2]

        entries = list(self.client.list_files("ws", "repo", "main"))

        self.assertEqual(len(entries), 2)
        self.assertEqual(self.mock_get.call_count, 2)
        second_url = self.mock_get.call_args_list[1].args[0]
        self.assertEqual(second_url, "https://api.bitbucket.org/2.0/page2")

    def test_list_files_stops_on_request_exception(self):
        self.mock_get.side_effect = requests.RequestException("timeout")
        entries = list(self.client.list_files("ws", "repo", "main"))
        self.assertEqual(entries, [])

    def test_list_files_first_request_uses_pagelen_param(self):
        self.mock_get.return_value = _api_response([])
        list(self.client.list_files("ws", "repo", "main"))
        call_kwargs = self.mock_get.call_args
        self.assertEqual(call_kwargs.kwargs["params"], {"pagelen": 100})

    def test_list_files_uses_correct_url(self):
        self.mock_get.return_value = _api_response([])
        list(self.client.list_files("myws", "myrepo", "develop"))
        url = self.mock_get.call_args.args[0]
        self.assertIn("myws/myrepo/src/develop", url)

    def test_get_file_content_returns_text(self):
        self.mock_get.return_value.raise_for_status = Mock()
        self.mock_get.return_value.text = "# Hello"
        result = self.client.get_file_content("ws", "repo", "main", "README.md")
        self.assertEqual(result, "# Hello")

    def test_get_file_content_uses_correct_url(self):
        self.mock_get.return_value.raise_for_status = Mock()
        self.mock_get.return_value.text = ""
        self.client.get_file_content("ws", "repo", "main", "docs/guide.md")
        url = self.mock_get.call_args.args[0]
        self.assertIn("ws/repo/src/main/docs/guide.md", url)

    def test_get_file_content_returns_empty_on_request_exception(self):
        self.mock_get.side_effect = requests.RequestException("403")
        result = self.client.get_file_content("ws", "repo", "main", "secret.md")
        self.assertEqual(result, "")


def _make_config(
    username="user",
    api_token="secret",
    workspace="myworkspace",
    repo="myrepo",
    branch="master",
    include_extensions="",
    exclude_extensions="",
    include_directories="",
    exclude_directories="",
):
    return {
        "name": "test_bitbucket",
        "config": {
            "username": username,
            "api_token": api_token,
            "workspace": workspace,
            "repo": repo,
            "branch": branch,
            "include_extensions": include_extensions,
            "exclude_extensions": exclude_extensions,
            "include_directories": include_directories,
            "exclude_directories": exclude_directories,
        },
    }


def _file_entry(path, date="2024-06-01T12:00:00+00:00"):
    return {"type": "commit_file", "path": path, "commit": {"date": date}}


class TestBitbucketIngestionJob(unittest.TestCase):
    def setUp(self):
        self.client_patcher = patch("tasks.bitbucket_ingestion.BitbucketClient")
        self.mock_client_class = self.client_patcher.start()
        self.mock_client = MagicMock()
        self.mock_client_class.return_value = self.mock_client

    def tearDown(self):
        self.client_patcher.stop()

    def _make_job(self, **kwargs):
        return BitbucketIngestionJob(_make_config(**kwargs))

    # ------------------------------------------------------------------
    # Initialisation & validation
    # ------------------------------------------------------------------

    def test_source_type(self):
        self.assertEqual(self._make_job().source_type, "bitbucket")

    def test_client_instantiated_with_credentials(self):
        self._make_job(username="alice", api_token="tok123")
        self.mock_client_class.assert_called_once_with("alice", "tok123")

    def test_missing_username_raises(self):
        with self.assertRaises(ValueError):
            BitbucketIngestionJob(_make_config(username=""))

    def test_missing_api_token_raises(self):
        with self.assertRaises(ValueError):
            BitbucketIngestionJob(_make_config(api_token=""))

    def test_missing_workspace_raises(self):
        with self.assertRaises(ValueError):
            BitbucketIngestionJob(_make_config(workspace=""))

    def test_missing_repo_raises(self):
        with self.assertRaises(ValueError):
            BitbucketIngestionJob(_make_config(repo=""))

    def test_default_branch_is_master(self):
        job = BitbucketIngestionJob(_make_config(branch=""))
        self.assertEqual(job.branch, "master")

    def test_include_and_exclude_extensions_mutually_exclusive(self):
        with self.assertRaises(ValueError):
            BitbucketIngestionJob(_make_config(include_extensions="md", exclude_extensions="txt"))

    def test_include_and_exclude_directories_mutually_exclusive(self):
        with self.assertRaises(ValueError):
            BitbucketIngestionJob(_make_config(include_directories="docs", exclude_directories="tests"))

    # ------------------------------------------------------------------
    # list_items
    # ------------------------------------------------------------------

    def test_list_items_yields_ingestion_items(self):
        self.mock_client.list_files.return_value = [
            _file_entry("README.md"),
            _file_entry("src/main.py"),
        ]
        items = list(self._make_job().list_items())

        self.assertEqual(len(items), 2)
        self.assertIsInstance(items[0], IngestionItem)
        self.assertEqual(items[0].source_ref, "README.md")
        self.assertEqual(items[1].source_ref, "src/main.py")

    def test_list_items_id_format(self):
        self.mock_client.list_files.return_value = [_file_entry("docs/index.md")]
        items = list(self._make_job().list_items())
        self.assertEqual(items[0].id, "bitbucket:myworkspace/myrepo/master/docs/index.md")

    def test_list_items_last_modified_parsed(self):
        self.mock_client.list_files.return_value = [_file_entry("README.md", date="2024-06-15T10:30:00+00:00")]
        items = list(self._make_job().list_items())
        self.assertIsNotNone(items[0].last_modified)
        self.assertEqual(items[0].last_modified.year, 2024)
        self.assertEqual(items[0].last_modified.month, 6)

    def test_list_items_calls_client_with_correct_args(self):
        self.mock_client.list_files.return_value = []
        list(self._make_job(workspace="ws", repo="repo", branch="develop").list_items())
        self.mock_client.list_files.assert_called_once_with("ws", "repo", "develop")

    def test_list_items_empty_result(self):
        self.mock_client.list_files.return_value = []
        self.assertEqual(list(self._make_job().list_items()), [])

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    def test_include_extensions_filters_out_non_matching(self):
        self.mock_client.list_files.return_value = [
            _file_entry("README.md"),
            _file_entry("script.py"),
        ]
        items = list(self._make_job(include_extensions="md").list_items())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].source_ref, "README.md")

    def test_exclude_extensions_filters_out_matching(self):
        self.mock_client.list_files.return_value = [
            _file_entry("README.md"),
            _file_entry("script.py"),
        ]
        items = list(self._make_job(exclude_extensions="py").list_items())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].source_ref, "README.md")

    def test_include_directories_filters_out_non_matching(self):
        self.mock_client.list_files.return_value = [
            _file_entry("docs/guide.md"),
            _file_entry("src/main.py"),
        ]
        items = list(self._make_job(include_directories="docs").list_items())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].source_ref, "docs/guide.md")

    def test_exclude_directories_filters_out_matching(self):
        self.mock_client.list_files.return_value = [
            _file_entry("docs/guide.md"),
            _file_entry("src/main.py"),
        ]
        items = list(self._make_job(exclude_directories="src").list_items())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].source_ref, "docs/guide.md")

    def test_include_directories_accepts_nested_paths(self):
        self.mock_client.list_files.return_value = [
            _file_entry("docs/api/v1/index.md"),
            _file_entry("other/file.md"),
        ]
        items = list(self._make_job(include_directories="docs").list_items())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].source_ref, "docs/api/v1/index.md")

    def test_include_extensions_and_include_directories_combined(self):
        self.mock_client.list_files.return_value = [
            _file_entry("docs/guide.md"),
            _file_entry("docs/notes.txt"),
            _file_entry("src/main.md"),
        ]
        items = list(self._make_job(include_extensions="md", include_directories="docs").list_items())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].source_ref, "docs/guide.md")

    def test_root_level_file_with_include_directory_excluded(self):
        self.mock_client.list_files.return_value = [_file_entry("README.md")]
        items = list(self._make_job(include_directories="docs").list_items())
        self.assertEqual(len(items), 0)

    # ------------------------------------------------------------------
    # get_raw_content
    # ------------------------------------------------------------------

    def test_get_raw_content_delegates_to_client(self):
        self.mock_client.get_file_content.return_value = "# Hello\nContent here."
        item = IngestionItem(id="bitbucket:ws/repo/master/README.md", source_ref="README.md")
        job = self._make_job()
        content = job.get_raw_content(item)

        self.mock_client.get_file_content.assert_called_once_with("myworkspace", "myrepo", "master", "README.md")
        self.assertIn("Hello", content)

    def test_get_raw_content_returns_empty_on_client_error(self):
        self.mock_client.get_file_content.return_value = ""
        item = IngestionItem(id="bitbucket:ws/repo/master/secret.md", source_ref="secret.md")
        self.assertEqual(self._make_job().get_raw_content(item), "")

    # ------------------------------------------------------------------
    # get_item_name
    # ------------------------------------------------------------------

    def test_get_item_name_returns_safe_string(self):
        item = IngestionItem(id="bitbucket:ws/repo/master/docs/index.md", source_ref="docs/index.md")
        name = self._make_job().get_item_name(item)
        self.assertNotIn("/", name)
        self.assertIn("docs", name)
        self.assertLessEqual(len(name), 255)

    def test_get_item_name_truncates_long_paths(self):
        long_path = "a/" * 200 + "file.md"
        item = IngestionItem(id=f"bitbucket:ws/repo/master/{long_path}", source_ref=long_path)
        self.assertLessEqual(len(self._make_job().get_item_name(item)), 255)

    # ------------------------------------------------------------------
    # get_extra_metadata
    # ------------------------------------------------------------------

    def test_get_extra_metadata_contains_required_fields(self):
        item = IngestionItem(
            id="bitbucket:myworkspace/myrepo/master/docs/guide.md",
            source_ref="docs/guide.md",
            last_modified=datetime(2024, 6, 1),
        )
        meta = self._make_job().get_extra_metadata(item=item, content="", metadata={})

        self.assertEqual(meta["workspace"], "myworkspace")
        self.assertEqual(meta["repo"], "myrepo")
        self.assertEqual(meta["branch"], "master")
        self.assertEqual(meta["path"], "docs/guide.md")
        self.assertEqual(meta["title"], "guide.md")
        self.assertEqual(meta["file_extension"], "md")
        self.assertIn("bitbucket.org", meta["url"])
        self.assertIn("docs/guide.md", meta["url"])

    def test_get_extra_metadata_root_file_no_extension(self):
        item = IngestionItem(id="bitbucket:ws/repo/master/Makefile", source_ref="Makefile")
        meta = self._make_job().get_extra_metadata(item=item, content="", metadata={})
        self.assertEqual(meta["file_extension"], "")
        self.assertEqual(meta["title"], "Makefile")

    # ------------------------------------------------------------------
    # _parse_csv
    # ------------------------------------------------------------------

    def test_parse_csv_splits_and_lowercases(self):
        self.assertEqual(BitbucketIngestionJob._parse_csv("MD, TXT, Py"), {"md", "txt", "py"})

    def test_parse_csv_empty_string_returns_empty_set(self):
        self.assertEqual(BitbucketIngestionJob._parse_csv(""), set())

    # ------------------------------------------------------------------
    # _parse_timestamp
    # ------------------------------------------------------------------

    def test_parse_timestamp_valid(self):
        result = BitbucketIngestionJob._parse_timestamp("2024-06-15T10:30:00+00:00")
        self.assertIsNotNone(result)
        self.assertEqual(result.year, 2024)

    def test_parse_timestamp_none_returns_none(self):
        self.assertIsNone(BitbucketIngestionJob._parse_timestamp(None))

    def test_parse_timestamp_invalid_returns_none(self):
        self.assertIsNone(BitbucketIngestionJob._parse_timestamp("not-a-date"))


if __name__ == "__main__":
    unittest.main()
