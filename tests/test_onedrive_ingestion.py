import unittest
from unittest.mock import MagicMock, patch

from llama_index.core import Document

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.onedrive_ingestion import OneDriveIngestionJob


def _make_config(**kwargs) -> dict:
    cfg = {
        "client_id": "test-client-id",
        "client_secret": "test-client-secret",
        "tenant_id": "test-tenant-id",
        "userprincipalname": "user@example.com",
    }
    cfg.update(kwargs)
    return {"name": "onedrive1", "config": cfg}


def _make_doc(
    file_id="file123", file_name="report.pdf", file_path="/Documents/report.pdf", text="Hello world"
) -> Document:
    doc = Document(text=text)
    doc.metadata = {
        "file_id": file_id,
        "file_name": file_name,
        "file_path": file_path,
    }
    return doc


class TestOneDriveIngestionJob(unittest.TestCase):
    def setUp(self):
        self.patcher = patch("tasks.onedrive_ingestion.OneDriveReader")
        self.mock_reader_class = self.patcher.start()
        self.mock_reader = MagicMock()
        self.mock_reader_class.return_value = self.mock_reader

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

    def test_defaults(self):
        job = self._make_job()
        self.assertEqual(job.tenant_id, "test-tenant-id")
        self.assertTrue(job.recursive)
        self.assertIsNone(job.folder_id)
        self.assertIsNone(job.folder_path)
        self.assertIsNone(job.file_ids)
        self.assertIsNone(job.file_paths)
        self.assertIsNone(job.mime_types)

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

    def test_list_items_yields_ingestion_items(self):
        doc1 = _make_doc(file_id="f1")
        doc2 = _make_doc(file_id="f2", file_name="notes.docx", file_path="/Notes/notes.docx")
        self.mock_reader.load_data.return_value = [doc1, doc2]

        job = self._make_job()
        items = list(job.list_items())

        self.assertEqual(len(items), 2)
        self.assertIsInstance(items[0], IngestionItem)
        self.assertEqual(items[0].id, "onedrive:f1")
        self.assertEqual(items[1].id, "onedrive:f2")

    def test_list_items_unique_ids_for_multipage_file(self):
        doc1 = _make_doc(file_id="f1", text="page 1")
        doc1.metadata["page_label"] = "1"
        doc2 = _make_doc(file_id="f1", text="page 2")
        doc2.metadata["page_label"] = "2"
        self.mock_reader.load_data.return_value = [doc1, doc2]

        job = self._make_job()
        items = list(job.list_items())

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].id, "onedrive:f1:1")
        self.assertEqual(items[1].id, "onedrive:f1:2")

    def test_list_items_passes_mime_types_and_recursive(self):
        self.mock_reader.load_data.return_value = []
        job = self._make_job(mime_types="application/pdf", recursive=False)
        list(job.list_items())

        self.mock_reader.load_data.assert_called_once_with(
            mime_types=["application/pdf"],
            recursive=False,
        )

    def test_list_items_empty_when_reader_returns_nothing(self):
        self.mock_reader.load_data.return_value = []
        job = self._make_job()
        items = list(job.list_items())
        self.assertEqual(items, [])

    def test_list_items_empty_when_reader_returns_none(self):
        self.mock_reader.load_data.return_value = None
        job = self._make_job()
        items = list(job.list_items())
        self.assertEqual(items, [])

    def test_list_items_raises_on_reader_exception(self):
        self.mock_reader.load_data.side_effect = RuntimeError("auth error")
        job = self._make_job()
        with self.assertRaises(RuntimeError):
            list(job.list_items())

    def test_list_items_source_ref_is_document(self):
        doc = _make_doc()
        self.mock_reader.load_data.return_value = [doc]
        job = self._make_job()
        items = list(job.list_items())
        self.assertIs(items[0].source_ref, doc)

    def test_init_instantiates_reader_with_config(self):
        self._make_job(folder_path="Documents/Reports")

        self.mock_reader_class.assert_called_once_with(
            client_id="test-client-id",
            client_secret="test-client-secret",
            tenant_id="test-tenant-id",
            userprincipalname="user@example.com",
            folder_id=None,
            file_ids=None,
            folder_path="Documents/Reports",
            file_paths=None,
        )

    def test_get_raw_content_returns_document_text(self):
        doc = _make_doc(text="Some file content")
        item = IngestionItem(id="onedrive:f1", source_ref=doc)
        job = self._make_job()
        content = job.get_raw_content(item)
        self.assertEqual(content, "Some file content")

    def test_get_raw_content_caches_metadata(self):
        doc = _make_doc(file_id="f99", file_name="x.pdf", file_path="/x.pdf")
        item = IngestionItem(id="onedrive:f99", source_ref=doc)
        job = self._make_job()
        job.get_raw_content(item)
        self.assertEqual(item._metadata_cache["file_id"], "f99")
        self.assertEqual(item._metadata_cache["file_name"], "x.pdf")
        self.assertEqual(item._metadata_cache["file_path"], "/x.pdf")

    def test_get_raw_content_empty_text_returns_empty_string(self):
        doc = _make_doc(text="")
        item = IngestionItem(id="onedrive:f1", source_ref=doc)
        job = self._make_job()
        self.assertEqual(job.get_raw_content(item), "")

    def test_get_item_name_sanitizes_path(self):
        doc = _make_doc(file_path="/Documents/My Reports/Q1 Report.pdf")
        item = IngestionItem(id="onedrive:f1", source_ref=doc)
        job = self._make_job()
        name = job.get_item_name(item)
        self.assertNotIn("/", name)
        self.assertNotIn(" ", name)

    def test_get_item_name_truncates_to_255(self):
        doc = _make_doc(file_path="/" + "a" * 300 + ".pdf")
        item = IngestionItem(id="onedrive:long", source_ref=doc)
        job = self._make_job()
        self.assertLessEqual(len(job.get_item_name(item)), 255)

    def test_get_extra_metadata_includes_file_fields(self):
        doc = _make_doc(file_id="fmeta", file_name="meta.pdf", file_path="/meta.pdf")
        doc.metadata["page_label"] = "3"
        item = IngestionItem(id="onedrive:fmeta:3", source_ref=doc)
        job = self._make_job()
        job.get_raw_content(item)  # populate cache
        extra = job.get_extra_metadata(item, "some content", {})
        self.assertEqual(extra["file_id"], "fmeta")
        self.assertEqual(extra["file_name"], "meta.pdf")
        self.assertEqual(extra["file_path"], "/meta.pdf")
        self.assertEqual(extra["page_label"], "3")


if __name__ == "__main__":
    unittest.main()
