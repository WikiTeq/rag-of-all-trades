import unittest
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from tasks.box_ingestion import BoxIngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem


def _make_config(**kwargs) -> dict:
    cfg = {
        "box_client_id": "test-client-id",
        "box_client_secret": "test-client-secret",
        "box_enterprise_id": "test-enterprise-id",
        "folder_id": "0",
    }
    cfg.update(kwargs)
    return {"name": "box1", "config": cfg}


def _make_box_file(
    file_id="file123", name="report.pdf", path_collection="All Files/Reports", modified_at="2024-01-15T10:30:00+00:00"
):
    box_file = MagicMock()
    box_file.id = file_id
    box_file.name = name
    box_file.modified_at = MagicMock()
    box_file.modified_at.isoformat.return_value = modified_at
    box_file.path_collection = MagicMock()
    box_file.path_collection.entries = [MagicMock(name=p) for p in path_collection.split("/")]
    return box_file


class TestBoxIngestionJob(unittest.TestCase):
    def setUp(self):
        self.reader_patcher = patch("tasks.box_ingestion.BoxReader")
        self.mock_reader_class = self.reader_patcher.start()
        self.mock_reader = MagicMock()
        self.mock_reader_class.return_value = self.mock_reader

        self.get_files_patcher = patch("tasks.box_ingestion.get_box_files_details")
        self.mock_get_files = self.get_files_patcher.start()

        self.get_content_patcher = patch("tasks.box_ingestion.get_file_content_by_id")
        self.mock_get_content = self.get_content_patcher.start()
        self.mock_get_content.return_value = b"file content"

        self.meta_patcher = patch("tasks.box_ingestion.box_file_to_llama_document_metadata")
        self.mock_meta = self.meta_patcher.start()

        self.box_sdk_patcher = patch.dict(
            "sys.modules",
            {
                "box_sdk_gen": MagicMock(
                    BoxCCGAuth=MagicMock(return_value=MagicMock()),
                    BoxJWTAuth=MagicMock(return_value=MagicMock()),
                    BoxClient=MagicMock(return_value=MagicMock()),
                    CCGConfig=MagicMock(return_value=MagicMock()),
                    JWTConfig=MagicMock(return_value=MagicMock()),
                ),
            },
        )
        self.box_sdk_patcher.start()

    def tearDown(self):
        self.reader_patcher.stop()
        self.get_files_patcher.stop()
        self.get_content_patcher.stop()
        self.meta_patcher.stop()
        self.box_sdk_patcher.stop()

    def _make_job(self, **kwargs) -> BoxIngestionJob:
        return BoxIngestionJob(_make_config(**kwargs))

    def test_missing_client_id_raises(self):
        with self.assertRaises(ValueError):
            BoxIngestionJob(
                {"name": "x", "config": {"box_client_secret": "s", "box_enterprise_id": "e", "folder_id": "0"}}
            )

    def test_missing_client_secret_raises(self):
        with self.assertRaises(ValueError):
            BoxIngestionJob(
                {"name": "x", "config": {"box_client_id": "id", "box_enterprise_id": "e", "folder_id": "0"}}
            )

    def test_no_ingestion_mode_raises(self):
        with self.assertRaises(ValueError):
            BoxIngestionJob(
                {"name": "x", "config": {"box_client_id": "id", "box_client_secret": "s", "box_enterprise_id": "e"}}
            )

    def test_ccg_user_id_only_accepted(self):
        job = BoxIngestionJob(
            {
                "name": "x",
                "config": {"box_client_id": "id", "box_client_secret": "s", "box_user_id": "u", "folder_id": "0"},
            }
        )
        self.assertIsNotNone(job.box_client)

    def test_ccg_missing_enterprise_and_user_id_raises(self):
        with self.assertRaises(ValueError):
            BoxIngestionJob(
                {"name": "x", "config": {"box_client_id": "id", "box_client_secret": "s", "folder_id": "0"}}
            )

    def test_invalid_auth_type_raises(self):
        with self.assertRaises(ValueError):
            self._make_job(auth_type="oauth")

    def test_defaults(self):
        job = self._make_job()
        self.assertEqual(job.folder_id, "0")
        self.assertFalse(job.is_recursive)
        self.assertIsNone(job.file_ids)
        self.assertIsNone(job.box_user_id)
        self.assertIsNone(job.search_query)
        self.assertIsNone(job.metadata_template)

    def test_file_ids_from_comma_string(self):
        job = self._make_job(folder_id=None, file_ids="id1, id2, id3")
        self.assertEqual(job.file_ids, ["id1", "id2", "id3"])

    def test_is_recursive_bool_parsing(self):
        self.assertTrue(self._make_job(is_recursive="true").is_recursive)
        self.assertFalse(self._make_job(is_recursive="false").is_recursive)
        self.assertTrue(self._make_job(is_recursive="1").is_recursive)

    def test_folder_and_file_ids_can_coexist(self):
        job = self._make_job(folder_id="0", file_ids="123")
        self.assertEqual(job.folder_id, "0")
        self.assertEqual(job.file_ids, ["123"])

    def test_search_query_alone_is_valid(self):
        job = self._make_job(folder_id=None, search_query="quarterly report")
        self.assertEqual(job.search_query, "quarterly report")

    def test_metadata_search_alone_is_valid(self):
        job = self._make_job(
            folder_id=None,
            metadata_template="enterprise_12345.myTemplate",
            metadata_ancestor_folder_id="0",
        )
        self.assertEqual(job.metadata_template, "enterprise_12345.myTemplate")

    def test_metadata_search_requires_both_template_and_folder(self):
        with self.assertRaises(ValueError):
            self._make_job(folder_id=None, metadata_template="t")

    def test_jwt_auth_type_accepted(self):
        job = self._make_job(
            auth_type="jwt",
            box_jwt_key_id="key1",
            box_private_key="-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----",
            box_private_key_passphrase="passphrase",
        )
        self.assertIsNotNone(job.box_client)

    def test_jwt_missing_key_id_raises(self):
        with self.assertRaises(ValueError):
            self._make_job(
                auth_type="jwt",
                box_private_key="key",
                box_private_key_passphrase="pass",
            )

    def test_list_items_folder_mode(self):
        f1, f2 = _make_box_file("f1"), _make_box_file("f2", name="notes.docx")
        self.mock_reader.list_resources.return_value = ["f1", "f2"]
        self.mock_get_files.return_value = [f1, f2]

        items = list(self._make_job().list_items())

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].id, "box:f1")
        self.assertEqual(items[1].id, "box:f2")
        self.assertIsInstance(items[0], IngestionItem)

    def test_list_items_file_ids_mode(self):
        box_file = _make_box_file("abc")
        self.mock_reader.list_resources.return_value = ["abc"]
        self.mock_get_files.return_value = [box_file]

        job = self._make_job(folder_id=None, file_ids="abc")
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "box:abc")
        self.mock_reader.list_resources.assert_called_once_with(folder_id=None, file_ids=["abc"], is_recursive=False)

    def test_list_items_passes_is_recursive(self):
        self.mock_reader.list_resources.return_value = []
        list(self._make_job(is_recursive="true").list_items())
        self.mock_reader.list_resources.assert_called_once_with(folder_id="0", file_ids=None, is_recursive=True)

    def test_list_items_search_mode(self):
        box_file = _make_box_file("s1")
        self.mock_reader.search_resources.return_value = ["s1"]
        self.mock_get_files.return_value = [box_file]

        job = self._make_job(folder_id=None, search_query="quarterly")
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.mock_reader.search_resources.assert_called_once_with(
            query="quarterly",
            file_extensions=None,
            ancestor_folder_ids=None,
        )

    def test_list_items_metadata_search_mode(self):
        box_file = _make_box_file("m1")
        self.mock_reader.search_resources_by_metadata.return_value = ["m1"]
        self.mock_get_files.return_value = [box_file]

        job = self._make_job(
            folder_id=None,
            metadata_template="enterprise_123.myTemplate",
            metadata_ancestor_folder_id="0",
            metadata_query="status = :status",
            metadata_query_params="status=active",
        )
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.mock_reader.search_resources_by_metadata.assert_called_once_with(
            from_="enterprise_123.myTemplate",
            ancestor_folder_id="0",
            query="status = :status",
            query_params={"status": "active"},
        )

    def test_list_items_combined_modes_deduplicates(self):
        f1, f2 = _make_box_file("f1"), _make_box_file("s1")
        self.mock_reader.list_resources.return_value = ["f1"]
        self.mock_reader.search_resources.return_value = ["f1", "s1"]  # f1 duplicate
        self.mock_get_files.return_value = [f1, f2]

        job = self._make_job(folder_id="0", search_query="quarterly")
        items = list(job.list_items())

        self.assertEqual(len(items), 2)
        self.mock_get_files.assert_called_once()
        args = self.mock_get_files.call_args
        self.assertEqual(len(args.kwargs.get("file_ids", args.args[1] if len(args.args) > 1 else [])), 2)

    def test_list_items_empty(self):
        self.mock_reader.list_resources.return_value = []
        self.assertEqual(list(self._make_job().list_items()), [])

    def test_list_items_raises_on_reader_error(self):
        self.mock_reader.list_resources.side_effect = RuntimeError("auth failed")
        with self.assertRaises(RuntimeError):
            list(self._make_job().list_items())

    def test_list_items_parses_last_modified(self):
        box_file = _make_box_file(modified_at="2024-06-01T12:00:00+00:00")
        self.mock_reader.list_resources.return_value = ["file123"]
        self.mock_get_files.return_value = [box_file]
        items = list(self._make_job().list_items())
        self.assertEqual(items[0].last_modified, datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC))

    def test_list_items_falls_back_to_now_on_missing_modified_at(self):
        box_file = _make_box_file()
        box_file.modified_at = None
        self.mock_reader.list_resources.return_value = ["file123"]
        self.mock_get_files.return_value = [box_file]
        before = datetime.now(UTC)
        items = list(self._make_job().list_items())
        after = datetime.now(UTC)
        self.assertIsNotNone(items[0].last_modified.tzinfo)
        self.assertLessEqual(
            abs((items[0].last_modified - before).total_seconds()), (after - before).total_seconds() + 1
        )

    def test_list_items_source_ref_is_box_file(self):
        box_file = _make_box_file()
        self.mock_reader.list_resources.return_value = ["file123"]
        self.mock_get_files.return_value = [box_file]
        items = list(self._make_job().list_items())
        self.assertIs(items[0].source_ref, box_file)

    def test_get_raw_content_returns_decoded_bytes(self):
        box_file = _make_box_file(file_id="f1")
        self.mock_get_content.return_value = b"Box file content"
        self.mock_meta.return_value = {"box_file_id": "f1", "name": "report.pdf", "path_collection": "All Files"}
        item = IngestionItem(id="box:f1", source_ref=box_file)
        self.assertEqual(self._make_job().get_raw_content(item), "Box file content")

    def test_get_raw_content_populates_metadata_cache(self):
        box_file = _make_box_file(file_id="f99", name="x.pdf")
        self.mock_get_content.return_value = b"content"
        self.mock_meta.return_value = {"box_file_id": "f99", "name": "x.pdf", "path_collection": "All Files"}
        item = IngestionItem(id="box:f99", source_ref=box_file)
        self._make_job().get_raw_content(item)
        self.assertEqual(item._metadata_cache["box_file_id"], "f99")
        self.assertEqual(item._metadata_cache["box_file_name"], "x.pdf")
        self.assertEqual(item._metadata_cache["path_collection"], "All Files")

    def test_get_item_name_basic(self):
        box_file = _make_box_file(file_id="abc123", name="my report.pdf")
        item = IngestionItem(id="box:abc123", source_ref=box_file)
        name = self._make_job().get_item_name(item)
        self.assertLessEqual(len(name), 255)
        self.assertIn("abc123", name)

    def test_get_item_name_truncates_to_255(self):
        box_file = _make_box_file(file_id="x", name="a" * 300)
        item = IngestionItem(id="box:x", source_ref=box_file)
        self.assertLessEqual(len(self._make_job().get_item_name(item)), 255)

    def test_get_extra_metadata(self):
        box_file = _make_box_file(file_id="meta1", name="doc.pdf")
        self.mock_get_content.return_value = b"content"
        self.mock_meta.return_value = {"box_file_id": "meta1", "name": "doc.pdf", "path_collection": "All Files/Docs"}
        item = IngestionItem(id="box:meta1", source_ref=box_file)
        job = self._make_job()
        job.get_raw_content(item)
        extra = job.get_extra_metadata(item, "content", {})
        self.assertEqual(extra["box_file_id"], "meta1")
        self.assertEqual(extra["box_file_name"], "doc.pdf")
        self.assertEqual(extra["path_collection"], "All Files/Docs")

    def test_parse_kv_pairs(self):
        result = BoxIngestionJob._parse_kv_pairs("status=active,owner=alice")
        self.assertEqual(result, {"status": "active", "owner": "alice"})

    def test_parse_kv_pairs_empty(self):
        self.assertEqual(BoxIngestionJob._parse_kv_pairs(""), {})


if __name__ == "__main__":
    unittest.main()
