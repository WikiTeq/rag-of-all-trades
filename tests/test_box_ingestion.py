import unittest
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from llama_index.core import Document

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


def _make_doc(
    file_id="file123",
    name="report.pdf",
    path_collection="All Files/Reports",
    text="Hello world",
    modified_at="2024-01-15T10:30:00Z",
) -> Document:
    doc = Document(text=text)
    doc.metadata = {
        "box_file_id": file_id,
        "name": name,
        "path_collection": path_collection,
        "modified_at": modified_at,
    }
    return doc


class TestBoxIngestionJob(unittest.TestCase):
    def setUp(self):
        self.reader_patcher = patch("tasks.box_ingestion.BoxReader")
        self.mock_reader_class = self.reader_patcher.start()
        self.mock_reader = MagicMock()
        self.mock_reader_class.return_value = self.mock_reader

        self.ccg_patcher = patch("tasks.box_ingestion.BoxCCGAuth", create=True)
        self.client_patcher = patch("tasks.box_ingestion.BoxClient", create=True)

        # patch inside the function's local import scope
        self.box_sdk_patcher = patch.dict(
            "sys.modules",
            {
                "box_sdk_gen": MagicMock(
                    BoxCCGAuth=MagicMock(return_value=MagicMock()),
                    BoxClient=MagicMock(return_value=MagicMock()),
                    CCGConfig=MagicMock(return_value=MagicMock()),
                ),
            },
        )
        self.box_sdk_patcher.start()

    def tearDown(self):
        self.reader_patcher.stop()
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

    def test_missing_enterprise_id_raises(self):
        with self.assertRaises(ValueError):
            BoxIngestionJob(
                {"name": "x", "config": {"box_client_id": "id", "box_client_secret": "s", "folder_id": "0"}}
            )

    def test_missing_folder_and_file_ids_raises(self):
        with self.assertRaises(ValueError):
            BoxIngestionJob(
                {"name": "x", "config": {"box_client_id": "id", "box_client_secret": "s", "box_enterprise_id": "e"}}
            )

    def test_defaults(self):
        job = self._make_job()
        self.assertEqual(job.folder_id, "0")
        self.assertFalse(job.is_recursive)
        self.assertIsNone(job.file_ids)
        self.assertIsNone(job.box_user_id)

    def test_file_ids_from_comma_string(self):
        job = self._make_job(folder_id=None, file_ids="id1, id2, id3")
        self.assertEqual(job.file_ids, ["id1", "id2", "id3"])

    def test_is_recursive_bool_parsing(self):
        self.assertTrue(self._make_job(is_recursive="true").is_recursive)
        self.assertFalse(self._make_job(is_recursive="false").is_recursive)
        self.assertTrue(self._make_job(is_recursive="1").is_recursive)

    def test_list_items_folder_mode(self):
        doc1 = _make_doc(file_id="f1")
        doc2 = _make_doc(file_id="f2", name="notes.docx")
        self.mock_reader.load_data.return_value = [doc1, doc2]

        items = list(self._make_job().list_items())

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].id, "box:f1")
        self.assertEqual(items[1].id, "box:f2")
        self.assertIsInstance(items[0], IngestionItem)

    def test_list_items_file_ids_mode(self):
        doc = _make_doc(file_id="abc")
        self.mock_reader.load_data.return_value = [doc]

        job = self._make_job(folder_id=None, file_ids="abc")
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "box:abc")
        self.mock_reader.load_data.assert_called_once_with(folder_id=None, file_ids=["abc"], is_recursive=False)

    def test_list_items_passes_is_recursive(self):
        self.mock_reader.load_data.return_value = []
        list(self._make_job(is_recursive="true").list_items())
        self.mock_reader.load_data.assert_called_once_with(folder_id="0", file_ids=None, is_recursive=True)

    def test_list_items_empty(self):
        self.mock_reader.load_data.return_value = []
        self.assertEqual(list(self._make_job().list_items()), [])

    def test_list_items_none_result(self):
        self.mock_reader.load_data.return_value = None
        self.assertEqual(list(self._make_job().list_items()), [])

    def test_list_items_raises_on_reader_error(self):
        self.mock_reader.load_data.side_effect = RuntimeError("auth failed")
        with self.assertRaises(RuntimeError):
            list(self._make_job().list_items())

    def test_list_items_parses_last_modified(self):
        doc = _make_doc(modified_at="2024-06-01T12:00:00Z")
        self.mock_reader.load_data.return_value = [doc]
        items = list(self._make_job().list_items())
        self.assertEqual(items[0].last_modified, datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC))

    def test_list_items_falls_back_to_now_on_missing_modified_at(self):
        doc = _make_doc(modified_at=None)
        doc.metadata.pop("modified_at", None)
        doc.metadata.pop("content_modified_at", None)
        self.mock_reader.load_data.return_value = [doc]
        items = list(self._make_job().list_items())
        self.assertIsNotNone(items[0].last_modified)

    def test_list_items_page_label_suffix(self):
        doc = _make_doc(file_id="f1")
        doc.metadata["page_label"] = "3"
        self.mock_reader.load_data.return_value = [doc]
        items = list(self._make_job().list_items())
        self.assertEqual(items[0].id, "box:f1:3")

    def test_list_items_source_ref_is_document(self):
        doc = _make_doc()
        self.mock_reader.load_data.return_value = [doc]
        items = list(self._make_job().list_items())
        self.assertIs(items[0].source_ref, doc)

    def test_get_raw_content_returns_text(self):
        doc = _make_doc(text="Box file content")
        item = IngestionItem(id="box:f1", source_ref=doc)
        self.assertEqual(self._make_job().get_raw_content(item), "Box file content")

    def test_get_raw_content_populates_metadata_cache(self):
        doc = _make_doc(file_id="f99", name="x.pdf", path_collection="All Files")
        item = IngestionItem(id="box:f99", source_ref=doc)
        self._make_job().get_raw_content(item)
        self.assertEqual(item._metadata_cache["box_file_id"], "f99")
        self.assertEqual(item._metadata_cache["box_file_name"], "x.pdf")
        self.assertEqual(item._metadata_cache["path_collection"], "All Files")

    def test_get_item_name_basic(self):
        doc = _make_doc(file_id="abc123", name="my report.pdf")
        item = IngestionItem(id="box:abc123", source_ref=doc)
        name = self._make_job().get_item_name(item)
        self.assertLessEqual(len(name), 255)
        self.assertIn("abc123", name)

    def test_get_item_name_truncates_to_255(self):
        doc = _make_doc(file_id="x", name="a" * 300)
        item = IngestionItem(id="box:x", source_ref=doc)
        self.assertLessEqual(len(self._make_job().get_item_name(item)), 255)

    def test_get_item_name_preserves_page_suffix(self):
        doc = _make_doc(file_id="f1", name="a" * 300)
        doc.metadata["page_label"] = "5"
        item = IngestionItem(id="box:f1:5", source_ref=doc)
        name = self._make_job().get_item_name(item)
        self.assertLessEqual(len(name), 255)
        self.assertTrue(name.endswith(":5"))

    def test_get_extra_metadata(self):
        doc = _make_doc(file_id="meta1", name="doc.pdf", path_collection="All Files/Docs")
        item = IngestionItem(id="box:meta1", source_ref=doc)
        self._make_job().get_raw_content(item)
        extra = self._make_job().get_extra_metadata(item, "content", {})
        self.assertEqual(extra["box_file_id"], "meta1")
        self.assertEqual(extra["box_file_name"], "doc.pdf")
        self.assertEqual(extra["path_collection"], "All Files/Docs")

    def test_parse_config_list_comma_string(self):
        self.assertEqual(BoxIngestionJob._parse_config_list("a, b, c"), ["a", "b", "c"])

    def test_parse_config_list_none(self):
        self.assertIsNone(BoxIngestionJob._parse_config_list(None))

    def test_parse_config_list_from_list(self):
        self.assertEqual(BoxIngestionJob._parse_config_list(["x", "y"]), ["x", "y"])


if __name__ == "__main__":
    unittest.main()
