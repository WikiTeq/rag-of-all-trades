import unittest
from datetime import UTC, datetime
from unittest.mock import patch

from llama_index.core.schema import Document
from llama_index.readers.microsoft_sharepoint import SharePointType

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.sharepoint_ingestion import SharePointIngestionJob


def _make_job(**cfg_overrides):
    """Build a SharePointIngestionJob with required fields and optional overrides."""
    cfg = {
        "name": "sharepoint1",
        "config": {
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "tenant_id": "test-tenant-id",
            **cfg_overrides,
        },
    }
    return SharePointIngestionJob(cfg)


class TestSharePointIngestionInit(unittest.TestCase):
    def test_required_fields(self):
        job = _make_job(sharepoint_site_name="MySite")
        self.assertEqual(job.client_id, "test-client-id")
        self.assertEqual(job.client_secret, "test-client-secret")
        self.assertEqual(job.tenant_id, "test-tenant-id")
        self.assertEqual(job.sharepoint_site_name, "MySite")

    def test_missing_client_id_raises(self):
        with self.assertRaises(ValueError):
            SharePointIngestionJob({"name": "sp1", "config": {"client_secret": "s", "tenant_id": "t"}})

    def test_missing_client_secret_raises(self):
        with self.assertRaises(ValueError):
            SharePointIngestionJob({"name": "sp1", "config": {"client_id": "c", "tenant_id": "t"}})

    def test_missing_tenant_id_raises(self):
        with self.assertRaises(ValueError):
            SharePointIngestionJob({"name": "sp1", "config": {"client_id": "c", "client_secret": "s"}})

    def test_recursive_defaults_true(self):
        job = _make_job()
        self.assertTrue(job.recursive)

    def test_recursive_false_string(self):
        job = _make_job(recursive="false")
        self.assertFalse(job.recursive)

    def test_sharepoint_type_defaults_drive(self):
        job = _make_job()
        self.assertEqual(job.sharepoint_type, SharePointType.DRIVE)

    def test_sharepoint_type_page(self):
        job = _make_job(sharepoint_type="page")
        self.assertEqual(job.sharepoint_type, SharePointType.PAGE)

    def test_source_type(self):
        job = _make_job()
        self.assertEqual(job.source_type, "sharepoint")

    def test_optional_fields_none_when_empty(self):
        job = _make_job()
        self.assertIsNone(job.sharepoint_site_name)
        self.assertIsNone(job.sharepoint_folder_path)
        self.assertIsNone(job.drive_name)


class TestSharePointIngestionListItems(unittest.TestCase):
    def _make_doc(self, file_path="Documents/report.pdf", text="Hello SharePoint"):
        doc = Document(text=text, metadata={"file_path": file_path, "file_name": "report.pdf"})
        return doc

    def test_yields_one_item_per_document(self):
        docs = [self._make_doc("a.pdf"), self._make_doc("b.pdf")]
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_data.return_value = docs
            job = _make_job(sharepoint_site_name="MySite", sharepoint_folder_path="Docs")
            items = list(job.list_items())

        self.assertEqual(len(items), 2)

    def test_item_id_contains_source_name_and_path(self):
        docs = [self._make_doc("Docs/report.pdf")]
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_data.return_value = docs
            job = _make_job(sharepoint_site_name="MySite")
            items = list(job.list_items())

        self.assertIn("sharepoint:", items[0].id)
        self.assertIn("sharepoint1", items[0].id)

    def test_source_ref_is_document(self):
        doc = self._make_doc()
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_data.return_value = [doc]
            job = _make_job()
            items = list(job.list_items())

        self.assertIs(items[0].source_ref, doc)

    def test_folder_path_and_drive_name_passed_to_load_data(self):
        docs = [self._make_doc()]
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_data.return_value = docs
            job = _make_job(
                sharepoint_site_name="MySite",
                sharepoint_folder_path="Reports",
                drive_name="MyDrive",
            )
            list(job.list_items())

        call_kwargs = MockReader.return_value.load_data.call_args[1]
        self.assertEqual(call_kwargs["sharepoint_folder_path"], "Reports")
        self.assertEqual(call_kwargs["drive_name"], "MyDrive")

    def test_recursive_not_passed_for_page_type(self):
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_data.return_value = []
            job = _make_job(sharepoint_type="page")
            list(job.list_items())

        call_kwargs = MockReader.return_value.load_data.call_args[1]
        self.assertNotIn("recursive", call_kwargs)

    def test_empty_site_returns_no_items(self):
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_data.return_value = []
            job = _make_job()
            items = list(job.list_items())

        self.assertEqual(items, [])

    def test_last_modified_from_metadata_datetime(self):
        ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        doc = Document(text="x", metadata={"file_path": "a.pdf", "last_modified_datetime": ts})
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_data.return_value = [doc]
            job = _make_job()
            items = list(job.list_items())

        self.assertEqual(items[0].last_modified, ts)

    def test_last_modified_from_metadata_string(self):
        doc = Document(text="x", metadata={"file_path": "a.pdf", "last_modified_datetime": "2024-06-01T12:00:00+00:00"})
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_data.return_value = [doc]
            job = _make_job()
            items = list(job.list_items())

        self.assertIsNotNone(items[0].last_modified)

    def test_last_modified_fallback_when_missing(self):
        doc = Document(text="x", metadata={"file_path": "a.pdf"})
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_data.return_value = [doc]
            job = _make_job()
            items = list(job.list_items())

        self.assertIsNotNone(items[0].last_modified)
        self.assertIsNotNone(items[0].last_modified.tzinfo)


class TestSharePointIngestionGetRawContent(unittest.TestCase):
    def test_returns_document_text(self):
        doc = Document(text="SharePoint content here", metadata={})
        item = IngestionItem(id="sharepoint:sp1:file.docx", source_ref=doc)
        job = _make_job()
        self.assertEqual(job.get_raw_content(item), "SharePoint content here")

    def test_returns_empty_string_for_none_text(self):
        doc = Document(text=None, metadata={})
        item = IngestionItem(id="sharepoint:sp1:file.docx", source_ref=doc)
        job = _make_job()
        self.assertEqual(job.get_raw_content(item), "")


class TestSharePointIngestionGetItemName(unittest.TestCase):
    def test_safe_name_no_special_chars(self):
        doc = Document(text="", metadata={"file_path": "Documents/My Report.pdf"})
        item = IngestionItem(id="x", source_ref=doc)
        job = _make_job()
        name = job.get_item_name(item)
        self.assertNotIn("/", name)
        self.assertNotIn(" ", name)
        self.assertLessEqual(len(name), 255)

    def test_name_truncated_to_255(self):
        long_path = "a" * 300
        doc = Document(text="", metadata={"file_path": long_path})
        item = IngestionItem(id="x", source_ref=doc)
        job = _make_job()
        name = job.get_item_name(item)
        self.assertLessEqual(len(name), 255)


class TestSharePointIngestionGetExtraMetadata(unittest.TestCase):
    def test_metadata_fields(self):
        doc = Document(
            text="content",
            metadata={
                "file_path": "Docs/report.pdf",
                "file_name": "report.pdf",
                "url": "https://contoso.sharepoint.com/Docs/report.pdf",
                "title": "Quarterly Report",
            },
        )
        item = IngestionItem(id="x", source_ref=doc)
        job = _make_job()
        extra = job.get_extra_metadata(item, "content", {})
        self.assertEqual(extra["file_path"], "Docs/report.pdf")
        self.assertEqual(extra["file_name"], "report.pdf")
        self.assertEqual(extra["url"], "https://contoso.sharepoint.com/Docs/report.pdf")
        self.assertEqual(extra["title"], "Quarterly Report")

    def test_title_falls_back_to_file_name(self):
        doc = Document(text="content", metadata={"file_name": "report.pdf"})
        item = IngestionItem(id="x", source_ref=doc)
        job = _make_job()
        extra = job.get_extra_metadata(item, "content", {})
        self.assertEqual(extra["title"], "report.pdf")

    def test_missing_metadata_uses_empty_string(self):
        doc = Document(text="content", metadata={})
        item = IngestionItem(id="x", source_ref=doc)
        job = _make_job()
        extra = job.get_extra_metadata(item, "content", {})
        self.assertEqual(extra["file_path"], "")
        self.assertEqual(extra["url"], "")


if __name__ == "__main__":
    unittest.main()
