import unittest
from unittest.mock import MagicMock, patch

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.onedrive_ingestion import OneDriveIngestionJob
from utils.graph_client import GraphItemNotFoundError


def _make_config(**kwargs) -> dict:
    cfg = {
        "client_id": "test-client-id",
        "client_secret": "test-client-secret",
        "tenant_id": "test-tenant-id",
        "userprincipalname": "user@example.com",
    }
    cfg.update(kwargs)
    return {"name": "onedrive1", "config": cfg}


def _make_graph_item(
    item_id="file123",
    name="report.pdf",
    parent_path="/drive/root:/Documents",
    web_url="https://example.sharepoint.com/report.pdf",
    etag="etag-1",
    size=1024,
    last_modified="2024-01-01T00:00:00Z",
    is_folder=False,
    has_download_url=True,
) -> dict:
    item = {
        "id": item_id,
        "name": name,
        "parentReference": {"path": parent_path},
        "webUrl": web_url,
        "eTag": etag,
        "size": size,
        "lastModifiedDateTime": last_modified,
    }
    if is_folder:
        item["folder"] = {}
    else:
        item["file"] = {"mimeType": "application/pdf"}
        if has_download_url:
            item["@microsoft.graph.downloadUrl"] = f"https://download.example.com/{item_id}"
    return item


class TestOneDriveIngestionJob(unittest.TestCase):
    def setUp(self):
        self.patcher = patch("tasks.onedrive_ingestion.GraphClient")
        self.mock_graph_class = self.patcher.start()
        self.mock_graph = MagicMock()
        self.mock_graph_class.return_value = self.mock_graph
        self.mock_graph.get_user_drive_id.return_value = "drive-1"

    def tearDown(self):
        self.patcher.stop()

    def _make_job(self, **kwargs) -> OneDriveIngestionJob:
        return OneDriveIngestionJob(_make_config(**kwargs))

    def test_source_type(self):
        job = self._make_job()
        self.assertEqual(job.source_type, "onedrive")

    def test_missing_client_id_raises(self):
        with self.assertRaises(ValueError):
            OneDriveIngestionJob({"name": "x", "config": {}})

    def test_missing_client_secret_raises(self):
        with self.assertRaises(ValueError):
            OneDriveIngestionJob(
                {"name": "x", "config": {"client_id": "id", "tenant_id": "t", "userprincipalname": "u@x.com"}}
            )

    def test_missing_tenant_id_raises(self):
        with self.assertRaises(ValueError):
            OneDriveIngestionJob(
                {"name": "x", "config": {"client_id": "id", "client_secret": "s", "userprincipalname": "u@x.com"}}
            )

    def test_missing_userprincipalname_raises(self):
        with self.assertRaises(ValueError):
            OneDriveIngestionJob({"name": "x", "config": {"client_id": "id", "client_secret": "s", "tenant_id": "t"}})

    def test_defaults(self):
        job = self._make_job()
        self.assertEqual(job.tenant_id, "test-tenant-id")
        self.assertTrue(job.recursive)
        self.assertIsNone(job.folder_id)
        self.assertIsNone(job.folder_path)
        self.assertEqual(job.file_ids, [])
        self.assertEqual(job.file_paths, [])
        self.assertEqual(job.mime_types, [])

    def test_file_ids_parsed_from_comma_string(self):
        job = self._make_job(file_ids="id1, id2, id3")
        self.assertEqual(job.file_ids, ["id1", "id2", "id3"])

    def test_file_paths_parsed_from_comma_string(self):
        job = self._make_job(file_paths="/docs/a.pdf, /docs/b.pdf")
        self.assertEqual(job.file_paths, ["/docs/a.pdf", "/docs/b.pdf"])

    def test_mime_types_parsed_from_comma_string(self):
        job = self._make_job(mime_types="application/pdf, text/plain")
        self.assertEqual(job.mime_types, ["application/pdf", "text/plain"])

    def test_recursive_false(self):
        job = self._make_job(recursive=False)
        self.assertFalse(job.recursive)

    def test_recursive_string_values(self):
        self.assertFalse(self._make_job(recursive="false").recursive)
        self.assertFalse(self._make_job(recursive="0").recursive)
        self.assertTrue(self._make_job(recursive="true").recursive)

    def test_max_file_size_mb_default(self):
        self._make_job()
        _, kwargs = self.mock_graph_class.call_args
        self.assertEqual(kwargs["max_file_size_bytes"], 50 * 1024 * 1024)

    def test_max_file_size_mb_configured(self):
        self._make_job(max_file_size_mb=10)
        _, kwargs = self.mock_graph_class.call_args
        self.assertEqual(kwargs["max_file_size_bytes"], 10 * 1024 * 1024)

    def test_max_file_size_mb_invalid_raises(self):
        with self.assertRaises(ValueError):
            self._make_job(max_file_size_mb=0)

    def test_init_instantiates_graph_client_with_credentials(self):
        self._make_job()
        _, kwargs = self.mock_graph_class.call_args
        self.assertEqual(kwargs["client_id"], "test-client-id")
        self.assertEqual(kwargs["client_secret"], "test-client-secret")  # noqa: S105
        self.assertEqual(kwargs["tenant_id"], "test-tenant-id")


class TestOneDriveListItems(unittest.TestCase):
    def setUp(self):
        self.patcher = patch("tasks.onedrive_ingestion.GraphClient")
        self.mock_graph_class = self.patcher.start()
        self.mock_graph = MagicMock()
        self.mock_graph_class.return_value = self.mock_graph
        self.mock_graph.get_user_drive_id.return_value = "drive-1"

    def tearDown(self):
        self.patcher.stop()

    def _make_job(self, **kwargs) -> OneDriveIngestionJob:
        return OneDriveIngestionJob(_make_config(**kwargs))

    def test_list_items_walks_drive_root_by_default(self):
        self.mock_graph.get_drive_root.return_value = {"id": "root-1"}
        self.mock_graph.list_children.return_value = [_make_graph_item(item_id="f1")]

        job = self._make_job()
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertIsInstance(items[0], IngestionItem)
        self.assertEqual(items[0].id, "onedrive:drive-1:f1")
        self.mock_graph.get_drive_root.assert_called_once_with("drive-1")
        self.mock_graph.list_children.assert_called_once_with("drive-1", "root-1")

    def test_list_items_resolves_folder_id_before_walking(self):
        """folder_id is resolved via get_item first, not used directly as root_item_id —
        so a missing/mistyped folder_id fails the job instead of silently walking nothing
        (see test_list_items_raises_when_folder_id_not_found)."""
        self.mock_graph.get_item.return_value = _make_graph_item(item_id="folder-42", is_folder=True)
        self.mock_graph.list_children.return_value = [_make_graph_item(item_id="f1")]

        job = self._make_job(folder_id="folder-42")
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.mock_graph.get_drive_root.assert_not_called()
        self.mock_graph.get_item.assert_called_once_with("drive-1", "folder-42")
        self.mock_graph.list_children.assert_called_once_with("drive-1", "folder-42")

    def test_list_items_raises_when_folder_id_not_found(self):
        """A mistyped or deleted configured folder_id must fail the job — not be treated
        like a subfolder that disappeared mid-walk (which is safe to skip)."""
        self.mock_graph.get_item.side_effect = GraphItemNotFoundError("folder not found")

        job = self._make_job(folder_id="typo-folder-id")
        with self.assertRaises(GraphItemNotFoundError):
            list(job.list_items())

        self.mock_graph.list_children.assert_not_called()

    def test_list_items_resolves_folder_path(self):
        self.mock_graph.get_item_by_path.return_value = {"id": "resolved-folder"}
        self.mock_graph.list_children.return_value = []

        job = self._make_job(folder_path="Documents/Reports")
        list(job.list_items())

        self.mock_graph.get_item_by_path.assert_called_once_with("drive-1", "Documents/Reports")
        self.mock_graph.list_children.assert_called_once_with("drive-1", "resolved-folder")

    def test_list_items_recurses_into_subfolders(self):
        subfolder = _make_graph_item(item_id="sub1", is_folder=True)
        file_in_sub = _make_graph_item(item_id="f2")
        self.mock_graph.get_drive_root.return_value = {"id": "root-1"}
        self.mock_graph.list_children.side_effect = [
            [subfolder],
            [file_in_sub],
        ]

        job = self._make_job()
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "onedrive:drive-1:f2")
        self.assertEqual(self.mock_graph.list_children.call_count, 2)

    def test_list_items_recursive_false_skips_subfolders(self):
        subfolder = _make_graph_item(item_id="sub1", is_folder=True)
        top_level_file = _make_graph_item(item_id="f1")
        self.mock_graph.get_drive_root.return_value = {"id": "root-1"}
        self.mock_graph.list_children.return_value = [subfolder, top_level_file]

        job = self._make_job(recursive=False)
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "onedrive:drive-1:f1")
        self.mock_graph.list_children.assert_called_once()

    def test_list_items_skips_non_downloadable_items(self):
        no_url = _make_graph_item(item_id="onenote1", has_download_url=False)
        self.mock_graph.get_drive_root.return_value = {"id": "root-1"}
        self.mock_graph.list_children.return_value = [no_url]

        job = self._make_job()
        items = list(job.list_items())

        self.assertEqual(items, [])

    def test_list_items_filters_by_mime_type(self):
        matching = _make_graph_item(item_id="f1")
        non_matching = _make_graph_item(item_id="f2")
        non_matching["file"] = {"mimeType": "text/plain"}
        self.mock_graph.get_drive_root.return_value = {"id": "root-1"}
        self.mock_graph.list_children.return_value = [matching, non_matching]

        job = self._make_job(mime_types="application/pdf")
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "onedrive:drive-1:f1")

    def test_list_items_file_ids_scope(self):
        self.mock_graph.get_item.return_value = _make_graph_item(item_id="f1")

        job = self._make_job(file_ids="f1")
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.mock_graph.get_item.assert_called_once_with("drive-1", "f1")
        self.mock_graph.get_drive_root.assert_not_called()

    def test_list_items_file_paths_scope(self):
        self.mock_graph.get_item_by_path.return_value = _make_graph_item(item_id="f1")

        job = self._make_job(file_paths="/docs/a.pdf")
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.mock_graph.get_item_by_path.assert_called_once_with("drive-1", "/docs/a.pdf")

    def test_list_items_deduplicates_across_scopes(self):
        same_item = _make_graph_item(item_id="f1")
        self.mock_graph.get_item.return_value = same_item
        self.mock_graph.get_drive_root.return_value = {"id": "root-1"}
        self.mock_graph.list_children.return_value = [same_item]

        job = self._make_job(file_ids="f1")
        items = list(job.list_items())

        self.assertEqual(len(items), 1)

    def test_list_items_raises_on_auth_failure(self):
        self.mock_graph.get_user_drive_id.side_effect = RuntimeError("auth error")

        job = self._make_job()
        with self.assertRaises(RuntimeError):
            list(job.list_items())

    def test_list_items_raises_on_listing_failure(self):
        self.mock_graph.get_drive_root.return_value = {"id": "root-1"}
        self.mock_graph.list_children.side_effect = RuntimeError("listing failed")

        job = self._make_job()
        with self.assertRaises(RuntimeError):
            list(job.list_items())

    def test_list_items_skips_deleted_subfolder_without_failing_job(self):
        """A 404 on one subfolder (deleted between discovery and the walk reaching it)
        must not abort the whole run — only that subtree is skipped."""
        subfolder = _make_graph_item(item_id="sub1", is_folder=True)
        sibling_file = _make_graph_item(item_id="f1")
        self.mock_graph.get_drive_root.return_value = {"id": "root-1"}
        self.mock_graph.list_children.side_effect = [
            [subfolder, sibling_file],
            GraphItemNotFoundError("folder gone"),
        ]

        job = self._make_job()
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "onedrive:drive-1:f1")

    def test_list_items_raises_on_auth_failure_mid_walk(self):
        """A non-404 failure (auth, 403, 429, 5xx) mid-walk must still fail the whole job —
        only 404 (item genuinely gone) is treated as skippable."""
        subfolder = _make_graph_item(item_id="sub1", is_folder=True)
        self.mock_graph.get_drive_root.return_value = {"id": "root-1"}
        self.mock_graph.list_children.side_effect = [
            [subfolder],
            RuntimeError("403 forbidden"),
        ]

        job = self._make_job()
        with self.assertRaises(RuntimeError):
            list(job.list_items())

    def test_list_items_empty_drive_yields_nothing(self):
        self.mock_graph.get_drive_root.return_value = {"id": "root-1"}
        self.mock_graph.list_children.return_value = []

        job = self._make_job()
        items = list(job.list_items())

        self.assertEqual(items, [])

    def test_list_items_source_ref_excludes_download_url(self):
        """source_ref must never carry a pre-authenticated download URL — process_item()
        logs the full IngestionItem (including source_ref) on failure."""
        self.mock_graph.get_drive_root.return_value = {"id": "root-1"}
        self.mock_graph.list_children.return_value = [_make_graph_item(item_id="f1")]

        job = self._make_job()
        items = list(job.list_items())

        self.assertNotIn("downloadUrl", str(items[0].source_ref))
        self.assertNotIn("@microsoft.graph.downloadUrl", items[0].source_ref)

    def test_list_items_resolves_remote_item(self):
        """The remoteItem facet on a listing is a stub — file/downloadUrl are not
        trusted from it. The real item is fetched via get_item before being yielded."""
        remote_stub = {
            "remoteItem": {
                "id": "remote-file-1",
                "parentReference": {"driveId": "other-drive"},
                # Stub deliberately has no file/downloadUrl — real API doesn't reliably
                # provide them here either.
            }
        }
        resolved_remote_item = _make_graph_item(item_id="remote-file-1")
        self.mock_graph.get_drive_root.return_value = {"id": "root-1"}
        self.mock_graph.list_children.return_value = [remote_stub]
        self.mock_graph.get_item.return_value = resolved_remote_item

        job = self._make_job()
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "onedrive:other-drive:remote-file-1")
        self.mock_graph.get_item.assert_called_once_with("other-drive", "remote-file-1")

    def test_list_items_skips_remote_item_when_resolution_fails(self):
        remote_stub = {
            "remoteItem": {
                "id": "remote-file-1",
                "parentReference": {"driveId": "other-drive"},
            }
        }
        self.mock_graph.get_drive_root.return_value = {"id": "root-1"}
        self.mock_graph.list_children.return_value = [remote_stub]
        self.mock_graph.get_item.side_effect = GraphItemNotFoundError("gone")

        job = self._make_job()
        items = list(job.list_items())

        self.assertEqual(items, [])

    def test_list_items_recurses_into_remote_folder(self):
        """A remoteItem pointing at a folder is recursed into without an extra get_item
        call — folder/file detection there is reliable, unlike downloadUrl on a file."""
        remote_folder_stub = {
            "remoteItem": {
                "id": "remote-folder-1",
                "folder": {},
                "parentReference": {"driveId": "other-drive"},
            }
        }
        file_in_remote_folder = _make_graph_item(item_id="f-remote")
        self.mock_graph.get_drive_root.return_value = {"id": "root-1"}
        self.mock_graph.list_children.side_effect = [
            [remote_folder_stub],
            [file_in_remote_folder],
        ]

        job = self._make_job()
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "onedrive:other-drive:f-remote")
        self.mock_graph.get_item.assert_not_called()


class TestOneDriveGetItemChecksum(unittest.TestCase):
    def setUp(self):
        self.patcher = patch("tasks.onedrive_ingestion.GraphClient")
        self.mock_graph_class = self.patcher.start()
        self.mock_graph = MagicMock()
        self.mock_graph_class.return_value = self.mock_graph

    def tearDown(self):
        self.patcher.stop()

    def _make_job(self, **kwargs) -> OneDriveIngestionJob:
        return OneDriveIngestionJob(_make_config(**kwargs))

    def test_returns_etag_from_source_ref(self):
        job = self._make_job()
        item = IngestionItem(id="onedrive:drive-1:f1", source_ref={"etag": "etag-abc"})
        self.assertEqual(job.get_item_checksum(item), "etag-abc")

    def test_returns_none_when_etag_missing(self):
        job = self._make_job()
        item = IngestionItem(id="onedrive:drive-1:f1", source_ref={"etag": ""})
        self.assertIsNone(job.get_item_checksum(item))


class TestOneDriveGetRawContent(unittest.TestCase):
    def setUp(self):
        self.patcher = patch("tasks.onedrive_ingestion.GraphClient")
        self.mock_graph_class = self.patcher.start()
        self.mock_graph = MagicMock()
        self.mock_graph_class.return_value = self.mock_graph

    def tearDown(self):
        self.patcher.stop()

    def _make_job(self, **kwargs) -> OneDriveIngestionJob:
        return OneDriveIngestionJob(_make_config(**kwargs))

    def _make_item(self, **overrides) -> IngestionItem:
        ref = {
            "drive_id": "drive-1",
            "item_id": "f1",
            "name": "report.pdf",
            "parent_path": "/drive/root:/Documents",
            "web_url": "https://example.com/report.pdf",
            "etag": "etag-1",
            "size": 1024,
        }
        ref.update(overrides)
        return IngestionItem(id="onedrive:drive-1:f1", source_ref=ref)

    def test_fetches_fresh_download_url_and_downloads(self):
        self.mock_graph.get_download_url.return_value = "https://download.example.com/f1"
        self.mock_graph.download_content.return_value = b"file bytes"

        job = self._make_job()
        job.convert_to_markdown = MagicMock(return_value="# Converted")

        content = job.get_raw_content(self._make_item())

        self.assertEqual(content, "# Converted")
        self.mock_graph.get_download_url.assert_called_once_with("drive-1", "f1")
        self.mock_graph.download_content.assert_called_once_with("https://download.example.com/f1")

    def test_passes_file_extension_hint_to_conversion(self):
        self.mock_graph.get_download_url.return_value = "https://download.example.com/f1"
        self.mock_graph.download_content.return_value = b"file bytes"

        job = self._make_job()
        job.convert_to_markdown = MagicMock(return_value="# Converted")

        job.get_raw_content(self._make_item(name="report.pdf"))

        _, kwargs = job.convert_to_markdown.call_args
        self.assertEqual(kwargs["file_extension"], ".pdf")

    def test_falls_back_to_decoded_bytes_when_conversion_empty(self):
        self.mock_graph.get_download_url.return_value = "https://download.example.com/f1"
        self.mock_graph.download_content.return_value = b"plain text content"

        job = self._make_job()
        job.convert_to_markdown = MagicMock(return_value="")

        content = job.get_raw_content(self._make_item())

        self.assertEqual(content, "plain text content")


class TestOneDriveGetItemName(unittest.TestCase):
    def setUp(self):
        self.patcher = patch("tasks.onedrive_ingestion.GraphClient")
        self.mock_graph_class = self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def _make_job(self, **kwargs) -> OneDriveIngestionJob:
        return OneDriveIngestionJob(_make_config(**kwargs))

    def test_returns_stable_drive_item_key(self):
        job = self._make_job()
        item = IngestionItem(id="onedrive:drive-1:f1", source_ref={})
        self.assertEqual(job.get_item_name(item), "onedrive:drive-1:f1")

    def test_name_unaffected_by_rename_or_move(self):
        """The key is drive_id:item_id — renaming/moving the file (changing name/path in
        source_ref) must not change the identity key."""
        job = self._make_job()
        item_before = IngestionItem(id="onedrive:drive-1:f1", source_ref={"name": "old.pdf", "parent_path": "/A"})
        item_after = IngestionItem(id="onedrive:drive-1:f1", source_ref={"name": "new.pdf", "parent_path": "/B"})
        self.assertEqual(job.get_item_name(item_before), job.get_item_name(item_after))


class TestOneDriveGetExtraMetadata(unittest.TestCase):
    def setUp(self):
        self.patcher = patch("tasks.onedrive_ingestion.GraphClient")
        self.mock_graph_class = self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def _make_job(self, **kwargs) -> OneDriveIngestionJob:
        return OneDriveIngestionJob(_make_config(**kwargs))

    def test_uses_non_reserved_file_name_field(self):
        """Must use onedrive_file_name, not file_name — BaseMetadataSchema reserves
        file_name and process_item() silently drops any extra-metadata key matching it."""
        job = self._make_job()
        item = IngestionItem(
            id="onedrive:drive-1:f1",
            source_ref={"name": "report.pdf", "parent_path": "/Documents", "web_url": "https://x/report.pdf"},
        )
        extra = job.get_extra_metadata(item, "content", {})
        self.assertEqual(extra["onedrive_file_name"], "report.pdf")
        self.assertNotIn("file_name", extra)

    def test_includes_file_path_and_web_url(self):
        job = self._make_job()
        item = IngestionItem(
            id="onedrive:drive-1:f1",
            source_ref={"name": "report.pdf", "parent_path": "/Documents", "web_url": "https://x/report.pdf"},
        )
        extra = job.get_extra_metadata(item, "content", {})
        self.assertEqual(extra["file_path"], "/Documents")
        self.assertEqual(extra["web_url"], "https://x/report.pdf")


if __name__ == "__main__":
    unittest.main()
