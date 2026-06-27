import unittest
from datetime import UTC, datetime
from unittest.mock import Mock, patch

from dropbox.exceptions import ApiError, AuthError, HttpError
from dropbox.files import FileMetadata, FolderMetadata, ListFolderResult
from markitdown import MarkItDownException

from tasks.dropbox_ingestion import DropboxIngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem


def _make_result(entries, has_more=False, cursor="c"):
    result = Mock(spec=ListFolderResult)
    result.entries = entries
    result.has_more = has_more
    result.cursor = cursor
    return result


def _listed_paths(job, entries):
    job.dbx.files_list_folder.return_value = _make_result(entries)
    return [e.path_lower for e in job._list_folder_recursive("")]


def _make_config(extra=None):
    cfg = {"name": "test_dropbox", "config": {"access_token": "fake-token"}}
    if extra:
        cfg["config"].update(extra)
    return cfg


def _make_file_entry(path_display, path_lower=None, file_id="id:abc123", client_modified=None):
    entry = Mock(spec=FileMetadata)
    entry.path_display = path_display
    entry.path_lower = path_lower or path_display.lower()
    entry.id = file_id
    entry.client_modified = client_modified or datetime(2024, 6, 1, tzinfo=UTC)
    entry.rev = "rev_default"
    entry.content_hash = "hash_default"
    return entry


class TestDropboxIngestionInit(unittest.TestCase):
    """Tests for constructor argument parsing."""

    def setUp(self):
        self.dropbox_patcher = patch("tasks.dropbox_ingestion.Dropbox")
        self.md_patcher = patch("tasks.dropbox_ingestion.MarkItDown")
        self.mock_dropbox_cls = self.dropbox_patcher.start()
        self.md_patcher.start()

    def tearDown(self):
        self.dropbox_patcher.stop()
        self.md_patcher.stop()

    def test_source_type(self):
        job = DropboxIngestionJob(_make_config())
        self.assertEqual(job.source_type, "dropbox")

    def test_missing_access_token_raises(self):
        with self.assertRaises(ValueError, msg="access_token required"):
            DropboxIngestionJob({"name": "x", "config": {}})

    def test_default_paths_is_root(self):
        job = DropboxIngestionJob(_make_config())
        self.assertEqual(job.paths, [""])

    def test_paths_from_list(self):
        job = DropboxIngestionJob(_make_config({"paths": ["/Docs", "/Wiki"]}))
        self.assertEqual(job.paths, ["/Docs", "/Wiki"])

    def test_paths_from_comma_string(self):
        job = DropboxIngestionJob(_make_config({"paths": "/Docs, /Wiki"}))
        self.assertEqual(job.paths, ["/Docs", "/Wiki"])

    def test_include_extensions_parsed(self):
        job = DropboxIngestionJob(_make_config({"include_extensions": "md, docx, PDF"}))
        self.assertEqual(job.include_extensions, {"md", "docx", "pdf"})
        self.assertIsNone(job.exclude_extensions)

    def test_exclude_extensions_parsed(self):
        job = DropboxIngestionJob(_make_config({"exclude_extensions": ".png,.jpg"}))
        self.assertEqual(job.exclude_extensions, {"png", "jpg"})
        self.assertIsNone(job.include_extensions)

    def test_include_and_exclude_extensions_raises(self):
        with self.assertRaises(ValueError):
            DropboxIngestionJob(
                _make_config(
                    {
                        "include_extensions": "md",
                        "exclude_extensions": "png",
                    }
                )
            )

    def test_include_directories_parsed(self):
        job = DropboxIngestionJob(_make_config({"include_directories": "source, test"}))
        self.assertEqual(job.include_directories, {"source", "test"})
        self.assertIsNone(job.exclude_directories)

    def test_exclude_directories_parsed(self):
        job = DropboxIngestionJob(_make_config({"exclude_directories": "node_modules"}))
        self.assertEqual(job.exclude_directories, {"node_modules"})
        self.assertIsNone(job.include_directories)

    def test_include_and_exclude_directories_raises(self):
        with self.assertRaises(ValueError):
            DropboxIngestionJob(
                _make_config(
                    {
                        "include_directories": "src",
                        "exclude_directories": "test",
                    }
                )
            )


class TestDropboxExtensionFilter(unittest.TestCase):
    def setUp(self):
        with patch("tasks.dropbox_ingestion.Dropbox"), patch("tasks.dropbox_ingestion.MarkItDown"):
            self.job_include = DropboxIngestionJob(_make_config({"include_extensions": "md,docx"}))
            self.job_exclude = DropboxIngestionJob(_make_config({"exclude_extensions": "png,jpg"}))
            self.job_none = DropboxIngestionJob(_make_config())

    def test_include_allows_matching(self):
        entries = [_make_file_entry("/docs/file.md"), _make_file_entry("/docs/file.docx")]
        paths = _listed_paths(self.job_include, entries)
        self.assertIn("/docs/file.md", paths)
        self.assertIn("/docs/file.docx", paths)

    def test_include_blocks_non_matching(self):
        entries = [_make_file_entry("/docs/file.png"), _make_file_entry("/docs/file")]
        paths = _listed_paths(self.job_include, entries)
        self.assertEqual(paths, [])

    def test_exclude_blocks_matching(self):
        entries = [_make_file_entry("/img/photo.png"), _make_file_entry("/img/photo.jpg")]
        paths = _listed_paths(self.job_exclude, entries)
        self.assertEqual(paths, [])

    def test_exclude_allows_non_matching(self):
        entries = [_make_file_entry("/docs/file.md")]
        paths = _listed_paths(self.job_exclude, entries)
        self.assertIn("/docs/file.md", paths)

    def test_no_filter_allows_all(self):
        entries = [_make_file_entry("/any/file.xyz"), _make_file_entry("/any/file.md")]
        paths = _listed_paths(self.job_none, entries)
        self.assertEqual(len(paths), 2)


class TestDropboxDirectoryFilter(unittest.TestCase):
    def setUp(self):
        with patch("tasks.dropbox_ingestion.Dropbox"), patch("tasks.dropbox_ingestion.MarkItDown"):
            self.job_include = DropboxIngestionJob(_make_config({"include_directories": "source,test"}))
            self.job_exclude = DropboxIngestionJob(_make_config({"exclude_directories": "node_modules"}))
            self.job_none = DropboxIngestionJob(_make_config())

    def test_include_allows_matching(self):
        entries = [_make_file_entry("/project/source/file.md"), _make_file_entry("/project/test/file.md")]
        paths = _listed_paths(self.job_include, entries)
        self.assertEqual(len(paths), 2)

    def test_include_blocks_non_matching(self):
        entries = [_make_file_entry("/project/other/file.md")]
        paths = _listed_paths(self.job_include, entries)
        self.assertEqual(paths, [])

    def test_exclude_blocks_matching(self):
        entries = [_make_file_entry("/project/node_modules/file.js")]
        paths = _listed_paths(self.job_exclude, entries)
        self.assertEqual(paths, [])

    def test_exclude_allows_others(self):
        entries = [_make_file_entry("/project/src/file.md")]
        paths = _listed_paths(self.job_exclude, entries)
        self.assertIn("/project/src/file.md", paths)

    def test_no_filter_allows_all(self):
        entries = [_make_file_entry("/any/folder/file.md")]
        paths = _listed_paths(self.job_none, entries)
        self.assertEqual(len(paths), 1)


class TestDropboxListItems(unittest.TestCase):
    def _make_result(self, entries, has_more=False, cursor="cursor1"):
        result = Mock(spec=ListFolderResult)
        result.entries = entries
        result.has_more = has_more
        result.cursor = cursor
        return result

    def setUp(self):
        self.dropbox_patcher = patch("tasks.dropbox_ingestion.Dropbox")
        self.md_patcher = patch("tasks.dropbox_ingestion.MarkItDown")
        self.mock_dropbox_cls = self.dropbox_patcher.start()
        self.md_patcher.start()
        self.mock_dbx = self.mock_dropbox_cls.return_value

    def tearDown(self):
        self.dropbox_patcher.stop()
        self.md_patcher.stop()

    def test_list_items_yields_files(self):
        entry = _make_file_entry("/Docs/file.md", file_id="id:1")
        self.mock_dbx.files_list_folder.return_value = self._make_result([entry])

        job = DropboxIngestionJob(_make_config())
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "id:1")
        self.assertEqual(items[0].source_ref, "/Docs/file.md")

    def test_list_items_last_modified_set(self):
        lm = datetime(2024, 3, 15, tzinfo=UTC)
        entry = _make_file_entry("/file.md", file_id="id:2", client_modified=lm)
        self.mock_dbx.files_list_folder.return_value = self._make_result([entry])

        job = DropboxIngestionJob(_make_config())
        items = list(job.list_items())

        self.assertEqual(items[0].last_modified, lm)

    def test_list_items_naive_datetime_gets_utc(self):
        lm = datetime(2024, 3, 15)  # naive
        entry = _make_file_entry("/file.md", file_id="id:3", client_modified=lm)
        self.mock_dbx.files_list_folder.return_value = self._make_result([entry])

        job = DropboxIngestionJob(_make_config())
        items = list(job.list_items())

        self.assertIsNotNone(items[0].last_modified.tzinfo)

    def test_list_items_deduplicates_across_paths(self):
        entry = _make_file_entry("/Docs/file.md", file_id="id:same")
        self.mock_dbx.files_list_folder.return_value = self._make_result([entry])

        job = DropboxIngestionJob(_make_config({"paths": ["/Docs", "/Other"]}))
        items = list(job.list_items())

        # Same file_id returned from two different folder calls — should be deduplicated
        self.assertEqual(len(items), 1)
        self.assertEqual(self.mock_dbx.files_list_folder.call_count, 2)

    def test_list_items_pagination(self):
        entry1 = _make_file_entry("/a.md", file_id="id:1")
        entry2 = _make_file_entry("/b.md", file_id="id:2")
        first = self._make_result([entry1], has_more=True, cursor="cur1")
        second = self._make_result([entry2], has_more=False)
        self.mock_dbx.files_list_folder.return_value = first
        self.mock_dbx.files_list_folder_continue.return_value = second

        job = DropboxIngestionJob(_make_config())
        items = list(job.list_items())

        self.assertEqual(len(items), 2)
        self.mock_dbx.files_list_folder_continue.assert_called_once_with("cur1")

    def test_list_items_skips_non_file_entries(self):
        folder_entry = Mock(spec=FolderMetadata)
        folder_entry.path_lower = "/docs"
        file_entry = _make_file_entry("/docs/readme.md", file_id="id:f1")
        self.mock_dbx.files_list_folder.return_value = self._make_result([folder_entry, file_entry])

        job = DropboxIngestionJob(_make_config())
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "id:f1")

    def test_list_items_extension_filter_applied(self):
        md_entry = _make_file_entry("/a.md", file_id="id:1")
        png_entry = _make_file_entry("/b.png", file_id="id:2")
        self.mock_dbx.files_list_folder.return_value = self._make_result([md_entry, png_entry])

        job = DropboxIngestionJob(_make_config({"include_extensions": "md"}))
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "id:1")

    def test_list_items_auth_error_returns_empty(self):
        self.mock_dbx.files_list_folder.side_effect = AuthError("req", "err")

        job = DropboxIngestionJob(_make_config())
        items = list(job.list_items())

        self.assertEqual(items, [])

    def test_list_items_root_path_normalised(self):
        self.mock_dbx.files_list_folder.return_value = self._make_result([])

        job = DropboxIngestionJob(_make_config({"paths": ["/"]}))
        list(job.list_items())

        self.mock_dbx.files_list_folder.assert_called_once_with("", recursive=True)

    def test_list_items_path_without_leading_slash_normalised(self):
        self.mock_dbx.files_list_folder.return_value = self._make_result([])

        job = DropboxIngestionJob(_make_config({"paths": ["Docs/Engineering"]}))
        list(job.list_items())

        self.mock_dbx.files_list_folder.assert_called_once_with("/Docs/Engineering", recursive=True)

    def test_list_items_root_file_excluded_by_directory_filter(self):
        entry = _make_file_entry("/file.md", path_lower="/file.md", file_id="id:root")
        self.mock_dbx.files_list_folder.return_value = self._make_result([entry])

        job = DropboxIngestionJob(_make_config({"paths": ["/"], "include_directories": "docs"}))
        items = list(job.list_items())

        self.assertEqual(items, [])
        self.mock_dbx.files_list_folder.assert_called_once_with("", recursive=True)

    def test_list_items_metadata_cache_populated(self):
        entry = _make_file_entry("/Docs/file.md", file_id="id:1")
        entry.rev = "abc123rev"
        entry.content_hash = "deadbeef"
        entry.path_lower = "/docs/file.md"
        self.mock_dbx.files_list_folder.return_value = self._make_result([entry])

        job = DropboxIngestionJob(_make_config())
        items = list(job.list_items())

        self.assertEqual(items[0]._metadata_cache["rev"], "abc123rev")
        self.assertEqual(items[0]._metadata_cache["content_hash"], "deadbeef")
        self.assertEqual(items[0]._metadata_cache["path_lower"], "/docs/file.md")


class TestDropboxGetRawContent(unittest.TestCase):
    def setUp(self):
        self.dropbox_patcher = patch("tasks.dropbox_ingestion.Dropbox")
        self.md_patcher = patch("tasks.dropbox_ingestion.MarkItDown")
        self.mock_dropbox_cls = self.dropbox_patcher.start()
        self.mock_md_cls = self.md_patcher.start()
        self.mock_dbx = self.mock_dropbox_cls.return_value
        self.mock_md = self.mock_md_cls.return_value

    def tearDown(self):
        self.dropbox_patcher.stop()
        self.md_patcher.stop()

    def _make_item(self, path="/Docs/file.md", path_lower=None):
        item = IngestionItem(id="id:1", source_ref=path)
        if path_lower is not None:
            item._metadata_cache["path_lower"] = path_lower
        return item

    def _mock_download(self, content: bytes):
        response = Mock()
        response.content = content
        self.mock_dbx.files_download.return_value = (Mock(), response)

    def test_downloads_via_path_lower(self):
        self._mock_download(b"text")
        self.mock_md.convert_stream.return_value = Mock(text_content="text")

        job = DropboxIngestionJob(_make_config())
        job.get_raw_content(self._make_item("/Docs/File.md", path_lower="/docs/file.md"))

        self.mock_dbx.files_download.assert_called_once_with("/docs/file.md")

    def test_returns_markdown_converted_text(self):
        self._mock_download(b"raw bytes")
        self.mock_md.convert_stream.return_value = Mock(text_content="Converted text")

        job = DropboxIngestionJob(_make_config())
        result = job.get_raw_content(self._make_item())

        self.assertEqual(result, "Converted text")

    def test_falls_back_on_empty_markdown(self):
        self._mock_download(b"raw text")
        self.mock_md.convert_stream.return_value = Mock(text_content="   ")

        job = DropboxIngestionJob(_make_config())
        result = job.get_raw_content(self._make_item())

        self.assertEqual(result, "raw text")

    def test_falls_back_on_conversion_error(self):
        self._mock_download(b"raw fallback")
        self.mock_md.convert_stream.side_effect = MarkItDownException("bad conversion")

        job = DropboxIngestionJob(_make_config())
        result = job.get_raw_content(self._make_item())

        self.assertEqual(result, "raw fallback")

    def test_returns_empty_on_api_error(self):
        error = ApiError("req", Mock(), "err", "en")
        self.mock_dbx.files_download.side_effect = error

        job = DropboxIngestionJob(_make_config())
        result = job.get_raw_content(self._make_item())

        self.assertEqual(result, "")

    def test_returns_empty_on_http_error(self):
        self.mock_dbx.files_download.side_effect = HttpError("req", 500, "Internal Server Error")

        job = DropboxIngestionJob(_make_config())
        result = job.get_raw_content(self._make_item())

        self.assertEqual(result, "")


class TestDropboxGetItemName(unittest.TestCase):
    def setUp(self):
        with patch("tasks.dropbox_ingestion.Dropbox"), patch("tasks.dropbox_ingestion.MarkItDown"):
            self.job = DropboxIngestionJob(_make_config())

    def _item(self, path):
        return IngestionItem(id="id:x", source_ref=path)

    def test_simple_path(self):
        name = self.job.get_item_name(self._item("/Docs/file.md"))
        self.assertEqual(name, "Docs_file.md")

    def test_strips_leading_slash(self):
        name = self.job.get_item_name(self._item("/README.md"))
        self.assertFalse(name.startswith("_"))

    def test_spaces_replaced(self):
        name = self.job.get_item_name(self._item("/My Docs/My File.docx"))
        self.assertNotIn(" ", name)

    def test_truncated_to_255(self):
        long_path = "/" + "a" * 300 + ".md"
        name = self.job.get_item_name(self._item(long_path))
        self.assertLessEqual(len(name), 255)

    def test_fallback_for_empty_result(self):
        # A path that sanitizes to empty should return fallback
        name = self.job.get_item_name(self._item(""))
        self.assertEqual(name, "dropbox_file")


class TestDropboxGetItemChecksum(unittest.TestCase):
    def setUp(self):
        with patch("tasks.dropbox_ingestion.Dropbox"), patch("tasks.dropbox_ingestion.MarkItDown"):
            self.job = DropboxIngestionJob(_make_config())

    def _item_with_cache(self, rev=None, content_hash=None):
        item = IngestionItem(id="id:x", source_ref="/file.md")
        if rev is not None:
            item._metadata_cache["rev"] = rev
        if content_hash is not None:
            item._metadata_cache["content_hash"] = content_hash
        return item

    def test_returns_rev_when_present(self):
        item = self._item_with_cache(rev="rev42", content_hash="hash99")
        self.assertEqual(self.job.get_item_checksum(item), "rev42")

    def test_falls_back_to_content_hash(self):
        item = self._item_with_cache(content_hash="hash99")
        self.assertEqual(self.job.get_item_checksum(item), "hash99")

    def test_returns_none_when_empty_cache(self):
        item = IngestionItem(id="id:x", source_ref="/file.md")
        self.assertIsNone(self.job.get_item_checksum(item))


class TestDropboxGetExtraMetadata(unittest.TestCase):
    def setUp(self):
        with patch("tasks.dropbox_ingestion.Dropbox"), patch("tasks.dropbox_ingestion.MarkItDown"):
            self.job = DropboxIngestionJob(_make_config())

    def test_metadata_includes_file_path(self):
        item = IngestionItem(id="id:2", source_ref="/Engineering/notes.md")
        meta = self.job.get_extra_metadata(item, "", {})
        self.assertEqual(meta["file_path"], "/Engineering/notes.md")

    def test_metadata_file_path_is_display_not_lower(self):
        item = IngestionItem(id="id:3", source_ref="/Engineering/Notes.md")
        item._metadata_cache["path_lower"] = "/engineering/notes.md"
        meta = self.job.get_extra_metadata(item, "", {})
        self.assertEqual(meta["file_path"], "/Engineering/Notes.md")


if __name__ == "__main__":
    unittest.main()
