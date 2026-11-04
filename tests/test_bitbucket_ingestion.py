import unittest
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

from tasks.bitbucket_ingestion import BitbucketClient, BitbucketIngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.parse import parse_list

DUMMY_TOKEN = "dummy-api-token-for-tests"  # noqa: S105


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
        self.get_patcher = patch("utils.http.RetrySession.get")
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

    def test_list_files_prunes_excluded_subdirectory_without_http_call(self):
        root = _api_response(
            [
                {"type": "commit_file", "path": "README.md"},
                {"type": "commit_directory", "path": "docs"},
                {"type": "commit_directory", "path": "vendor"},
            ]
        )
        docs = _api_response([{"type": "commit_file", "path": "docs/guide.md"}])
        self.mock_get.side_effect = [root, docs]

        entries = list(self.client.list_files("ws", "repo", "main", exclude_directories={"vendor"}))

        paths = {e["path"] for e in entries}
        self.assertEqual(paths, {"README.md", "docs/guide.md"})
        # Only root + docs listed; vendor/ never triggers an HTTP call.
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

    def test_list_files_raises_on_request_exception(self):
        self.mock_get.side_effect = Exception("timeout")
        with self.assertRaises(Exception):
            list(self.client.list_files("ws", "repo", "main"))

    def test_list_files_raises_on_http_error(self):
        resp = _api_response([])
        resp.raise_for_status.side_effect = Exception("401 Unauthorized")
        self.mock_get.return_value = resp
        with self.assertRaises(Exception):
            list(self.client.list_files("ws", "repo", "main"))

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
        self.mock_get.return_value.content = b"# Hello"
        result = self.client.get_file_content("ws", "repo", "main", "README.md")
        self.assertEqual(result, "# Hello")

    def test_get_file_content_decodes_utf8_regardless_of_response_encoding(self):
        self.mock_get.return_value.raise_for_status = Mock()
        self.mock_get.return_value.content = "You\u2019ll start".encode()
        self.mock_get.return_value.encoding = "ISO-8859-1"
        result = self.client.get_file_content("ws", "repo", "main", "README.md")
        self.assertEqual(result, "You\u2019ll start")

    def test_get_file_content_uses_correct_url(self):
        self.mock_get.return_value.raise_for_status = Mock()
        self.mock_get.return_value.content = b""
        self.client.get_file_content("ws", "repo", "main", "docs/guide.md")
        url = self.mock_get.call_args.args[0]
        self.assertIn("ws/repo/src/main/docs/guide.md", url)

    def test_get_file_content_returns_empty_on_request_exception(self):
        self.mock_get.side_effect = Exception("403")
        result = self.client.get_file_content("ws", "repo", "main", "secret.md")
        self.assertEqual(result, "")

    def test_get_default_branch_returns_mainbranch_name(self):
        resp = Mock()
        resp.raise_for_status = Mock()
        resp.json.return_value = {"mainbranch": {"name": "main"}}
        self.mock_get.return_value = resp
        result = self.client.get_default_branch("ws", "repo")
        self.assertEqual(result, "main")

    def test_get_default_branch_raises_on_request_exception(self):
        self.mock_get.side_effect = Exception("timeout")
        with self.assertRaises(RuntimeError):
            self.client.get_default_branch("ws", "repo")

    def test_get_default_branch_raises_when_mainbranch_missing(self):
        resp = Mock()
        resp.raise_for_status = Mock()
        resp.json.return_value = {}
        self.mock_get.return_value = resp
        with self.assertRaises(RuntimeError):
            self.client.get_default_branch("ws", "repo")

    def test_get_file_etag_returns_etag_header(self):
        self.mock_get.return_value.headers = {"ETag": '"abc123"'}
        result = self.client.get_file_etag("ws", "repo", "main", "docs/guide.md")
        self.assertEqual(result, "abc123")

    def test_get_file_etag_returns_none_for_weak_etag(self):
        self.mock_get.return_value.headers = {"ETag": 'W/"abc123"'}
        self.assertIsNone(self.client.get_file_etag("ws", "repo", "main", "docs/guide.md"))

    def test_get_file_etag_returns_none_when_header_missing(self):
        self.mock_get.return_value.headers = {}
        self.assertIsNone(self.client.get_file_etag("ws", "repo", "main", "docs/guide.md"))

    def test_get_file_etag_returns_none_on_request_exception(self):
        self.mock_get.side_effect = Exception("timeout")
        self.assertIsNone(self.client.get_file_etag("ws", "repo", "main", "docs/guide.md"))

    def test_get_file_etag_requests_one_byte_range_of_src_url(self):
        self.mock_get.return_value.headers = {"ETag": "x"}
        self.client.get_file_etag("myws", "myrepo", "develop", "docs/guide.md")
        url = self.mock_get.call_args.args[0]
        self.assertIn("myws/myrepo/src/develop/docs/guide.md", url)
        self.assertEqual(self.mock_get.call_args.kwargs["headers"]["Range"], "bytes=0-0")


def _make_config(
    username="user",
    api_token=DUMMY_TOKEN,
    workspace="myworkspace",
    repo="myrepo",
    branch="master",
    include_extensions="",
    exclude_extensions="",
    include_directories="",
    exclude_directories="",
    **extra,
):
    return {
        "name": "test_bitbucket",
        "config": {
            **extra,
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


def _file_entry(path, date="2024-06-01T12:00:00+00:00", attributes=None):
    entry = {"type": "commit_file", "path": path, "commit": {"date": date}}
    if attributes is not None:
        entry["attributes"] = attributes
    return entry


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
        self._make_job(username="alice", api_token="tok123")  # noqa: S106 - test fixture credential
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

    def test_configured_branch_skips_default_branch_lookup(self):
        job = BitbucketIngestionJob(_make_config(branch="develop"))
        self.assertEqual(job.branch, "develop")
        self.mock_client.get_default_branch.assert_not_called()

    def test_empty_branch_resolves_default_branch_via_client(self):
        self.mock_client.get_default_branch.return_value = "main"
        job = BitbucketIngestionJob(_make_config(branch=""))
        self.assertEqual(job.branch, "main")
        self.mock_client.get_default_branch.assert_called_once_with("myworkspace", "myrepo")

    def test_empty_branch_raises_when_default_branch_lookup_fails(self):
        self.mock_client.get_default_branch.side_effect = RuntimeError("API error")
        with self.assertRaises(ValueError):
            BitbucketIngestionJob(_make_config(branch=""))

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

    def test_list_items_last_modified_is_none(self):
        self.mock_client.list_files.return_value = [_file_entry("README.md")]
        items = list(self._make_job().list_items())
        self.assertIsNone(items[0].last_modified)

    def test_list_items_makes_no_per_file_requests(self):
        self.mock_client.list_files.return_value = [_file_entry("README.md")]
        list(self._make_job().list_items())
        self.mock_client.get_file_etag.assert_not_called()

    def test_list_items_calls_client_with_correct_args(self):
        self.mock_client.list_files.return_value = []
        list(self._make_job(workspace="ws", repo="repo", branch="develop").list_items())
        self.mock_client.list_files.assert_called_once_with("ws", "repo", "develop", exclude_directories=None)

    def test_include_directories_crawls_each_prefix_directly(self):
        self.mock_client.list_files.return_value = []
        list(self._make_job(include_directories="docs,src").list_items())
        self.assertEqual(self.mock_client.list_files.call_count, 2)
        called_paths = {call.args[3] for call in self.mock_client.list_files.call_args_list}
        self.assertEqual(called_paths, {"docs", "src"})

    def test_exclude_directories_passed_through_for_pruning(self):
        self.mock_client.list_files.return_value = []
        list(self._make_job(exclude_directories="tests,vendor").list_items())
        self.mock_client.list_files.assert_called_once_with(
            "myworkspace", "myrepo", "master", exclude_directories={"tests", "vendor"}
        )

    def test_include_directories_dedupes_overlapping_prefixes(self):
        # docs/api/v1/index.md would be listed once via the "docs" crawl and again
        # via the nested "docs/api" crawl; list_items() must yield it only once.
        self.mock_client.list_files.side_effect = [
            [_file_entry("docs/guide.md"), _file_entry("docs/api/v1/index.md")],
            [_file_entry("docs/api/v1/index.md")],
        ]
        items = list(self._make_job(include_directories="docs,docs/api").list_items())
        paths = sorted(item.source_ref for item in items)
        self.assertEqual(paths, ["docs/api/v1/index.md", "docs/guide.md"])

    def test_list_items_empty_result(self):
        self.mock_client.list_files.return_value = []
        self.assertEqual(list(self._make_job().list_items()), [])

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    def test_binary_attribute_skips_file(self):
        self.mock_client.list_files.return_value = [
            _file_entry("README.md"),
            _file_entry("logo.png", attributes=["binary"]),
        ]
        items = list(self._make_job().list_items())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].source_ref, "README.md")

    def test_missing_attributes_field_does_not_skip_file(self):
        self.mock_client.list_files.return_value = [_file_entry("README.md")]
        items = list(self._make_job().list_items())
        self.assertEqual(len(items), 1)

    def test_non_binary_attributes_do_not_skip_file(self):
        self.mock_client.list_files.return_value = [_file_entry("README.md", attributes=["link"])]
        items = list(self._make_job().list_items())
        self.assertEqual(len(items), 1)

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

    def test_include_directories_preserves_case(self):
        self.mock_client.list_files.return_value = [
            _file_entry("Docs/guide.md"),
            _file_entry("docs/guide.md"),
        ]
        items = list(self._make_job(include_directories="Docs").list_items())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].source_ref, "Docs/guide.md")

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
    # get_item_checksum
    # ------------------------------------------------------------------

    def test_get_item_checksum_returns_etag(self):
        self.mock_client.get_file_etag.return_value = '"abc123"'
        item = IngestionItem(id="bitbucket:ws/repo/master/README.md", source_ref="README.md")
        result = self._make_job().get_item_checksum(item)

        self.mock_client.get_file_etag.assert_called_once_with("myworkspace", "myrepo", "master", "README.md")
        self.assertEqual(result, '"abc123"')

    def test_get_item_checksum_returns_none_on_client_failure(self):
        self.mock_client.get_file_etag.return_value = None
        item = IngestionItem(id="bitbucket:ws/repo/master/README.md", source_ref="README.md")
        self.assertIsNone(self._make_job().get_item_checksum(item))

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

    def test_get_item_name_includes_hash_suffix(self):
        item = IngestionItem(id="bitbucket:ws/repo/master/README.md", source_ref="README.md")
        name = self._make_job().get_item_name(item)
        self.assertRegex(name, r"_[0-9a-f]{8}$")

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

    def test_get_extra_metadata_url_percent_encodes_special_characters(self):
        item = IngestionItem(
            id="bitbucket:myworkspace/myrepo/master/docs/release notes #1.md",
            source_ref="docs/release notes #1.md",
        )
        meta = self._make_job(branch="feature/foo").get_extra_metadata(item=item, content="", metadata={})
        self.assertIn("docs/release%20notes%20%231.md", meta["url"])
        self.assertIn("feature%2Ffoo", meta["url"])
        self.assertNotIn(" ", meta["url"])

    # ------------------------------------------------------------------
    # parse_list (replaces _parse_csv)
    # ------------------------------------------------------------------

    def test_parse_list_splits_and_lowercases(self):
        self.assertEqual(set(parse_list("MD, TXT, Py", lower=True)), {"md", "txt", "py"})

    def test_parse_list_empty_string_returns_empty(self):
        self.assertEqual(parse_list(""), [])


def _pr(pr_id=7, **overrides):
    pr = {
        "id": pr_id,
        "title": "Add search",
        "description": "Adds the search page.",
        "state": "OPEN",
        "reason": "",
        "draft": False,
        "author": {"display_name": "Alice"},
        "reviewers": [{"display_name": "Bob"}, {"display_name": "Carol"}],
        "source": {"branch": {"name": "feature/search"}},
        "destination": {"branch": {"name": "main"}},
        "created_on": "2026-11-01T10:00:00.000000+00:00",
        "updated_on": "2026-11-02T11:30:00.000000+00:00",
        "comment_count": 0,
    }
    pr.update(overrides)
    return pr


def _comment(raw, **overrides):
    comment = {
        "id": 1,
        "content": {"raw": raw},
        "user": {"display_name": "Bob"},
        "created_on": "2026-11-02T09:00:00.000000+00:00",
        "deleted": False,
    }
    comment.update(overrides)
    return comment


def _pr_item(pr):
    return IngestionItem(id=f"bitbucket:myworkspace/myrepo:pr:{pr['id']}", source_ref=pr)


class TestBitbucketClientPullRequests(unittest.TestCase):
    def setUp(self):
        self.get_patcher = patch("utils.http.RetrySession.get")
        self.mock_get = self.get_patcher.start()
        self.client = BitbucketClient("user", "token")

    def tearDown(self):
        self.get_patcher.stop()

    def test_list_pull_requests_sends_every_state_and_the_fields_filter(self):
        self.mock_get.return_value = _api_response([_pr(1)])
        list(self.client.list_pull_requests("ws", "repo", {"OPEN", "MERGED"}))
        call = self.mock_get.call_args
        self.assertTrue(call.args[0].endswith("/repositories/ws/repo/pullrequests"))
        params = call.kwargs["params"]
        self.assertEqual([v for k, v in params if k == "state"], ["MERGED", "OPEN"])
        self.assertIn(("pagelen", 50), params)
        self.assertIn("values.comment_count", dict(params)["fields"])

    def test_list_pull_requests_follows_next_cursor(self):
        next_url = "https://api.bitbucket.org/2.0/next-page"
        self.mock_get.side_effect = [_api_response([_pr(1)], next_url=next_url), _api_response([_pr(2)])]
        ids = [pr["id"] for pr in self.client.list_pull_requests("ws", "repo", {"OPEN"})]
        self.assertEqual(ids, [1, 2])
        self.assertEqual(self.mock_get.call_args.args[0], next_url)
        self.assertEqual(self.mock_get.call_args.kwargs["params"], {})

    def test_list_pull_requests_raises_on_http_error(self):
        resp = _api_response([])
        resp.raise_for_status.side_effect = RuntimeError("403 Forbidden")
        self.mock_get.return_value = resp
        with self.assertRaises(RuntimeError):
            list(self.client.list_pull_requests("ws", "repo", {"OPEN"}))

    def test_list_pull_request_comments_requests_one_capped_page(self):
        self.mock_get.return_value = _api_response([_comment("hi")])
        comments = self.client.list_pull_request_comments("ws", "repo", 7)
        self.assertEqual(len(comments), 1)
        call = self.mock_get.call_args
        self.assertTrue(call.args[0].endswith("/repositories/ws/repo/pullrequests/7/comments"))
        self.assertEqual(call.kwargs["params"]["pagelen"], 50)

    def test_list_pull_request_comments_warns_when_more_comments_exist(self):
        self.mock_get.return_value = _api_response([_comment("hi")], next_url="https://api.bitbucket.org/2.0/more")
        with self.assertLogs("tasks.bitbucket_ingestion", level="WARNING") as logs:
            comments = self.client.list_pull_request_comments("ws", "repo", 7)
        self.assertEqual(len(comments), 1)
        self.assertIn("more than 50 comments", logs.output[0])
        self.assertEqual(self.mock_get.call_count, 1)

    def test_list_pull_request_comments_raises_on_http_error(self):
        resp = _api_response([])
        resp.raise_for_status.side_effect = RuntimeError("500")
        self.mock_get.return_value = resp
        with self.assertRaises(RuntimeError):
            self.client.list_pull_request_comments("ws", "repo", 7)


class TestBitbucketPullRequestJob(unittest.TestCase):
    def setUp(self):
        self.client_patcher = patch("tasks.bitbucket_ingestion.BitbucketClient")
        self.mock_client = self.client_patcher.start().return_value
        self.mock_client.list_files.return_value = []
        self.mock_client.list_pull_requests.return_value = []

    def tearDown(self):
        self.client_patcher.stop()

    def _make_job(self, **extra):
        return BitbucketIngestionJob(_make_config(**extra))

    def test_pull_requests_are_off_by_default(self):
        job = self._make_job()
        self.assertFalse(job.include_pull_requests)
        self.assertFalse(job.include_pull_request_comments)
        list(job.list_items())
        self.mock_client.list_pull_requests.assert_not_called()

    def test_string_booleans_are_parsed(self):
        job = self._make_job(include_pull_requests="true", include_pull_request_comments="yes")
        self.assertTrue(job.include_pull_requests)
        self.assertTrue(job.include_pull_request_comments)

    def test_comments_without_pull_requests_raises(self):
        with self.assertRaises(ValueError):
            self._make_job(include_pull_request_comments="true")

    def test_default_states_leave_out_superseded(self):
        self.assertEqual(self._make_job(include_pull_requests=True).pull_request_states, {"OPEN", "MERGED", "DECLINED"})

    def test_states_are_case_insensitive(self):
        job = self._make_job(include_pull_requests=True, pull_request_states="Merged, superseded")
        self.assertEqual(job.pull_request_states, {"MERGED", "SUPERSEDED"})

    def test_unknown_state_raises(self):
        with self.assertRaises(ValueError):
            self._make_job(include_pull_requests=True, pull_request_states="open,closed")

    def test_list_items_yields_files_then_pull_requests(self):
        self.mock_client.list_files.return_value = [_file_entry("README.md")]
        self.mock_client.list_pull_requests.return_value = [_pr(7)]
        items = list(self._make_job(include_pull_requests=True).list_items())
        self.assertEqual(
            [i.id for i in items],
            ["bitbucket:myworkspace/myrepo/master/README.md", "bitbucket:myworkspace/myrepo:pr:7"],
        )
        self.assertEqual(items[1].source_ref["title"], "Add search")
        self.assertEqual(items[1].last_modified, datetime.fromisoformat("2026-11-02T11:30:00+00:00"))
        self.mock_client.list_pull_requests.assert_called_once_with(
            "myworkspace", "myrepo", {"OPEN", "MERGED", "DECLINED"}
        )

    def test_list_items_skips_pull_request_without_id(self):
        self.mock_client.list_pull_requests.return_value = [{"title": "no id"}, _pr(8)]
        items = list(self._make_job(include_pull_requests=True).list_items())
        self.assertEqual([i.source_ref["id"] for i in items], [8])

    def test_list_items_raises_when_pull_request_listing_fails(self):
        def failing():
            raise RuntimeError("403 Forbidden")
            yield

        self.mock_client.list_pull_requests.return_value = failing()
        with self.assertRaises(RuntimeError):
            list(self._make_job(include_pull_requests=True).list_items())

    def test_checksum_is_stable_and_follows_updated_on(self):
        job = self._make_job(include_pull_requests=True)
        first = job.get_item_checksum(_pr_item(_pr()))
        self.assertEqual(first, job.get_item_checksum(_pr_item(_pr())))
        self.assertNotEqual(first, job.get_item_checksum(_pr_item(_pr(updated_on="2026-11-03T08:00:00.000000+00:00"))))
        self.mock_client.get_file_etag.assert_not_called()

    def test_checksum_is_none_without_updated_on(self):
        job = self._make_job(include_pull_requests=True)
        self.assertIsNone(job.get_item_checksum(_pr_item(_pr(updated_on=None))))

    def test_checksum_includes_comment_count_only_when_comments_enabled(self):
        plain = self._make_job(include_pull_requests=True)
        with_comments = self._make_job(include_pull_requests=True, include_pull_request_comments=True)
        two, three = _pr_item(_pr(comment_count=2)), _pr_item(_pr(comment_count=3))
        self.assertEqual(plain.get_item_checksum(two), plain.get_item_checksum(three))
        self.assertNotEqual(with_comments.get_item_checksum(two), with_comments.get_item_checksum(three))
        self.assertNotEqual(plain.get_item_checksum(two), with_comments.get_item_checksum(two))

    def test_raw_content_renders_every_field(self):
        text = self._make_job(include_pull_requests=True).get_raw_content(_pr_item(_pr()))
        self.assertEqual(
            text,
            "Pull Request #7: Add search\n"
            "State: OPEN\n"
            "Author: Alice\n"
            "Reviewers: Bob, Carol\n"
            "Branch: feature/search -> main\n"
            "Created: 2026-11-01T10:00:00.000000+00:00\n"
            "Updated: 2026-11-02T11:30:00.000000+00:00\n"
            "\n"
            "Description:\n"
            "Adds the search page.",
        )

    def test_raw_content_marks_draft_and_shows_decline_reason(self):
        job = self._make_job(include_pull_requests=True)
        text = job.get_raw_content(_pr_item(_pr(state="DECLINED", draft=True, reason="Superseded by #9")))
        self.assertIn("State: DECLINED (draft)", text)
        self.assertIn("Reason: Superseded by #9", text)

    def test_raw_content_without_description_or_reviewers(self):
        text = self._make_job(include_pull_requests=True).get_raw_content(_pr_item(_pr(description="", reviewers=[])))
        self.assertNotIn("Description:", text)
        self.assertNotIn("Reviewers:", text)
        self.assertTrue(text.startswith("Pull Request #7: Add search"))

    def test_raw_content_does_not_fetch_comments_when_disabled(self):
        self._make_job(include_pull_requests=True).get_raw_content(_pr_item(_pr(comment_count=3)))
        self.mock_client.list_pull_request_comments.assert_not_called()

    def test_raw_content_does_not_fetch_comments_when_pull_request_has_none(self):
        job = self._make_job(include_pull_requests=True, include_pull_request_comments=True)
        job.get_raw_content(_pr_item(_pr(comment_count=0)))
        self.mock_client.list_pull_request_comments.assert_not_called()

    def test_raw_content_renders_comments_replies_and_inline_comments(self):
        self.mock_client.list_pull_request_comments.return_value = [
            _comment("Looks good"),
            _comment("Thanks", user={"display_name": "Alice"}, parent={"id": 1}),
            _comment("Rename this", inline={"path": "src/app.py", "to": 12}),
            _comment("Removed one", deleted=True),
            _comment("Unpublished draft", pending=True),
            _comment("   "),
        ]
        job = self._make_job(include_pull_requests=True, include_pull_request_comments=True)
        text = job.get_raw_content(_pr_item(_pr(comment_count=5)))
        self.mock_client.list_pull_request_comments.assert_called_once_with("myworkspace", "myrepo", 7)
        stamp = "2026-11-02T09:00:00.000000+00:00"
        self.assertTrue(
            text.endswith(
                "Comments:\n"
                f"- Bob ({stamp}): Looks good\n"
                f"- Alice ({stamp}) (reply): Thanks\n"
                f"- Bob ({stamp}) on src/app.py:12: Rename this"
            )
        )

    def test_comment_listing_failure_propagates(self):
        self.mock_client.list_pull_request_comments.side_effect = RuntimeError("500")
        job = self._make_job(include_pull_requests=True, include_pull_request_comments=True)
        with self.assertRaises(RuntimeError):
            job.get_raw_content(_pr_item(_pr(comment_count=1)))

    def test_item_name_is_unique_per_pull_request(self):
        job = self._make_job(include_pull_requests=True)
        self.assertNotEqual(job.get_item_name(_pr_item(_pr(7))), job.get_item_name(_pr_item(_pr(8))))
        self.assertTrue(job.get_item_name(_pr_item(_pr(7))).startswith("myworkspace_myrepo_pr_7_"))

    def test_item_name_cannot_collide_with_a_file_named_like_a_pull_request(self):
        job = self._make_job(branch="pr", include_pull_requests=True)
        file_item = IngestionItem(id="bitbucket:myworkspace/myrepo/pr/7", source_ref="7")
        self.assertNotEqual(job.get_item_name(file_item), job.get_item_name(_pr_item(_pr(7))))

    def test_extra_metadata_for_pull_request(self):
        job = self._make_job(include_pull_requests=True)
        meta = job.get_extra_metadata(_pr_item(_pr(comment_count=4)), "", {})
        self.assertEqual(
            meta,
            {
                "item_type": "pull_request",
                "url": "https://bitbucket.org/myworkspace/myrepo/pull-requests/7",
                "title": "Add search",
                "workspace": "myworkspace",
                "repo": "myrepo",
                "pr_id": 7,
                "state": "OPEN",
                "draft": False,
                "author": "Alice",
                "reviewers": ["Bob", "Carol"],
                "source_branch": "feature/search",
                "destination_branch": "main",
                "created_on": "2026-11-01T10:00:00.000000+00:00",
                "updated_on": "2026-11-02T11:30:00.000000+00:00",
                "comment_count": 4,
            },
        )

    def test_extra_metadata_for_file_has_file_item_type(self):
        item = IngestionItem(id="bitbucket:myworkspace/myrepo/master/a.md", source_ref="a.md")
        self.assertEqual(self._make_job().get_extra_metadata(item, "", {})["item_type"], "file")


if __name__ == "__main__":
    unittest.main()
