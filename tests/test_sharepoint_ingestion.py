import unittest
from datetime import UTC, datetime
from pathlib import Path
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
            "sharepoint_site_name": "MySite",
            **cfg_overrides,
        },
    }
    return SharePointIngestionJob(cfg)


def _make_info(
    file_path="MySite/Documents/report.pdf",
    modified_at="2024-06-01T12:00:00+00:00",
    url="https://sp.example.com/report.pdf",
):
    return {"file_path": file_path, "modified_at": modified_at, "url": url}


def _make_doc(file_path="Documents/report.pdf", text="Hello SharePoint"):
    return Document(text=text, metadata={"file_path": file_path, "file_name": Path(file_path).name})


class TestSharePointIngestionInit(unittest.TestCase):
    def test_required_fields(self):
        job = _make_job()
        self.assertEqual(job.client_id, "test-client-id")
        self.assertEqual(job.client_secret, "test-client-secret")
        self.assertEqual(job.tenant_id, "test-tenant-id")
        self.assertEqual(job.sharepoint_site_name, "MySite")

    def test_missing_client_id_raises(self):
        with self.assertRaises(ValueError):
            SharePointIngestionJob(
                {"name": "sp1", "config": {"client_secret": "s", "tenant_id": "t", "sharepoint_site_name": "S"}}
            )

    def test_missing_client_secret_raises(self):
        with self.assertRaises(ValueError):
            SharePointIngestionJob(
                {"name": "sp1", "config": {"client_id": "c", "tenant_id": "t", "sharepoint_site_name": "S"}}
            )

    def test_missing_tenant_id_raises(self):
        with self.assertRaises(ValueError):
            SharePointIngestionJob(
                {"name": "sp1", "config": {"client_id": "c", "client_secret": "s", "sharepoint_site_name": "S"}}
            )

    def test_recursive_defaults_true(self):
        job = _make_job()
        self.assertTrue(job.recursive)

    def test_recursive_false_string(self):
        job = _make_job(recursive="false")
        self.assertFalse(job.recursive)

    def test_recursive_off_string_is_false(self):
        job = _make_job(recursive="off")
        self.assertFalse(job.recursive)

    def test_sharepoint_type_defaults_drive(self):
        job = _make_job()
        self.assertEqual(job.sharepoint_type, SharePointType.DRIVE)

    def test_sharepoint_type_page(self):
        job = _make_job(sharepoint_type="page")
        self.assertEqual(job.sharepoint_type, SharePointType.PAGE)

    def test_invalid_sharepoint_type_raises(self):
        with self.assertRaises(ValueError):
            _make_job(sharepoint_type="invalid")

    def test_source_type(self):
        job = _make_job()
        self.assertEqual(job.source_type, "sharepoint")

    def test_optional_fields_none_when_empty(self):
        job = _make_job()
        self.assertIsNone(job.sharepoint_folder_path)
        self.assertIsNone(job.drive_name)

    def test_drive_name_passed_to_constructor(self):
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.list_resources.return_value = []
            job = _make_job(drive_name="MyDrive")
            list(job.list_items())
        constructor_kwargs = MockReader.call_args[1]
        self.assertEqual(constructor_kwargs["drive_name"], "MyDrive")
        self.assertNotIn("drive_name", constructor_kwargs.get("load_kwargs", {}))

    def test_missing_site_name_and_site_id_raises(self):
        with self.assertRaises(ValueError):
            SharePointIngestionJob(
                {
                    "name": "sp1",
                    "config": {
                        "client_id": "c",
                        "client_secret": "s",
                        "tenant_id": "t",
                    },
                }
            )

    def test_site_id_alone_is_sufficient(self):
        job = _make_job(sharepoint_site_name="", sharepoint_site_id="abc-123")
        self.assertEqual(job.sharepoint_site_id, "abc-123")
        self.assertIsNone(job.sharepoint_site_name)


class TestSharePointIngestionListItemsDrive(unittest.TestCase):
    def test_yields_one_item_per_resource(self):
        paths = [Path("MySite/Docs/a.pdf"), Path("MySite/Docs/b.pdf")]
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.list_resources.return_value = paths
            MockReader.return_value.get_resource_info.side_effect = [
                _make_info("MySite/Docs/a.pdf"),
                _make_info("MySite/Docs/b.pdf"),
            ]
            job = _make_job(sharepoint_folder_path="Docs")
            items = list(job.list_items())

        self.assertEqual(len(items), 2)

    def test_item_id_contains_source_name_and_path(self):
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.list_resources.return_value = [Path("MySite/Docs/report.pdf")]
            MockReader.return_value.get_resource_info.return_value = _make_info("MySite/Docs/report.pdf")
            job = _make_job()
            items = list(job.list_items())

        self.assertIn("sharepoint:", items[0].id)
        self.assertIn("sharepoint1", items[0].id)
        self.assertIn("MySite/Docs/report.pdf", items[0].id)

    def test_source_ref_is_info_dict(self):
        info = _make_info()
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.list_resources.return_value = [Path(info["file_path"])]
            MockReader.return_value.get_resource_info.return_value = info
            job = _make_job()
            items = list(job.list_items())

        self.assertIsInstance(items[0].source_ref, dict)
        self.assertEqual(items[0].source_ref["file_path"], info["file_path"])

    def test_load_data_not_called_for_drive(self):
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.list_resources.return_value = []
            job = _make_job()
            list(job.list_items())

        MockReader.return_value.load_data.assert_not_called()

    def test_folder_path_passed_to_list_resources(self):
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.list_resources.return_value = []
            job = _make_job(sharepoint_folder_path="Reports", drive_name="MyDrive")
            list(job.list_items())

        call_kwargs = MockReader.return_value.list_resources.call_args[1]
        self.assertEqual(call_kwargs["sharepoint_folder_path"], "Reports")
        self.assertTrue(call_kwargs["recursive"])

    def test_recursive_passed_to_list_resources(self):
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.list_resources.return_value = []
            job = _make_job(recursive="false")
            list(job.list_items())

        call_kwargs = MockReader.return_value.list_resources.call_args[1]
        self.assertFalse(call_kwargs["recursive"])

    def test_empty_drive_returns_no_items(self):
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.list_resources.return_value = []
            job = _make_job()
            items = list(job.list_items())

        self.assertEqual(items, [])

    def test_last_modified_from_modified_at(self):
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.list_resources.return_value = [Path("MySite/a.pdf")]
            MockReader.return_value.get_resource_info.return_value = _make_info(modified_at="2024-06-01T12:00:00+00:00")
            job = _make_job()
            items = list(job.list_items())

        self.assertEqual(items[0].last_modified, datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC))

    def test_last_modified_fallback_when_missing(self):
        info = {"file_path": "MySite/a.pdf", "url": "https://sp.example.com/a.pdf"}
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.list_resources.return_value = [Path("MySite/a.pdf")]
            MockReader.return_value.get_resource_info.return_value = info
            job = _make_job()
            items = list(job.list_items())

        self.assertIsNotNone(items[0].last_modified)
        self.assertIsNotNone(items[0].last_modified.tzinfo)

    def test_get_resource_info_failure_skips_item(self):
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.list_resources.return_value = [
                Path("MySite/bad.pdf"),
                Path("MySite/good.pdf"),
            ]
            MockReader.return_value.get_resource_info.side_effect = [
                Exception("API error"),
                _make_info("MySite/good.pdf"),
            ]
            job = _make_job()
            items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertIn("good.pdf", items[0].id)

    def test_missing_file_path_in_info_skips_item(self):
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.list_resources.return_value = [
                Path("MySite/no-path.pdf"),
                Path("MySite/good.pdf"),
            ]
            MockReader.return_value.get_resource_info.side_effect = [
                {"modified_at": "2024-06-01T12:00:00+00:00"},
                _make_info("MySite/good.pdf"),
            ]
            job = _make_job()
            items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertIn("good.pdf", items[0].id)

    def test_site_id_passed_to_list_resources(self):
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.list_resources.return_value = []
            job = _make_job(sharepoint_site_name="", sharepoint_site_id="abc-123")
            list(job.list_items())

        call_kwargs = MockReader.return_value.list_resources.call_args[1]
        self.assertEqual(call_kwargs["sharepoint_site_id"], "abc-123")
        self.assertNotIn("sharepoint_site_name", call_kwargs)


class TestSharePointIngestionListItemsPage(unittest.TestCase):
    def test_yields_one_item_per_page(self):
        docs = [_make_doc("page1.aspx", "Page 1"), _make_doc("page2.aspx", "Page 2")]
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_data.return_value = docs
            job = _make_job(sharepoint_type="page")
            items = list(job.list_items())

        self.assertEqual(len(items), 2)

    def test_source_ref_is_document_for_page(self):
        doc = _make_doc("page1.aspx")
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_data.return_value = [doc]
            job = _make_job(sharepoint_type="page")
            items = list(job.list_items())

        self.assertIs(items[0].source_ref, doc)

    def test_recursive_not_passed_for_page_type(self):
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_data.return_value = []
            job = _make_job(sharepoint_type="page")
            list(job.list_items())

        call_kwargs = MockReader.return_value.load_data.call_args[1]
        self.assertNotIn("recursive", call_kwargs)

    def test_site_id_passed_to_load_data(self):
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_data.return_value = []
            job = _make_job(sharepoint_type="page", sharepoint_site_name="", sharepoint_site_id="abc-123")
            list(job.list_items())

        call_kwargs = MockReader.return_value.load_data.call_args[1]
        self.assertEqual(call_kwargs["sharepoint_site_id"], "abc-123")
        self.assertNotIn("sharepoint_site_name", call_kwargs)

    def test_last_modified_from_camel_case_key(self):
        doc = Document(
            text="x",
            metadata={"file_path": "page.aspx", "lastModifiedDateTime": "2024-06-01T12:00:00+00:00"},
        )
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_data.return_value = [doc]
            job = _make_job(sharepoint_type="page")
            items = list(job.list_items())

        self.assertIsNotNone(items[0].last_modified)
        self.assertIsNotNone(items[0].last_modified.tzinfo)

    def test_missing_last_modified_uses_debug_not_warning(self):
        doc = Document(text="x", metadata={"file_path": "page.aspx"})
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_data.return_value = [doc]
            with self.assertLogs("tasks.sharepoint_ingestion", level="DEBUG") as cm:
                job = _make_job(sharepoint_type="page")
                list(job.list_items())

        debug_msgs = [m for m in cm.output if m.startswith("DEBUG:")]
        warning_msgs = [m for m in cm.output if "last_modified" in m and m.startswith("WARNING:")]
        self.assertTrue(any("last_modified" in m for m in debug_msgs))
        self.assertEqual(warning_msgs, [])


class TestSharePointIngestionGetRawContent(unittest.TestCase):
    def test_drive_calls_load_resource(self):
        doc = Document(text="File content", metadata={})
        info = _make_info("MySite/Docs/report.pdf")
        item = IngestionItem(id="sharepoint:sp1:MySite/Docs/report.pdf", source_ref=info)
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_resource.return_value = [doc]
            job = _make_job()
            job._reader = MockReader.return_value
            result = job.get_raw_content(item)

        MockReader.return_value.load_resource.assert_called_once_with("MySite/Docs/report.pdf")
        self.assertEqual(result, "File content")

    def test_drive_returns_empty_when_load_resource_empty(self):
        info = _make_info()
        item = IngestionItem(id="sharepoint:sp1:MySite/Docs/report.pdf", source_ref=info)
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_resource.return_value = []
            job = _make_job()
            job._reader = MockReader.return_value
            result = job.get_raw_content(item)

        self.assertEqual(result, "")

    def test_drive_returns_empty_string_for_none_text(self):
        doc = Document(text=None, metadata={})
        info = _make_info()
        item = IngestionItem(id="sharepoint:sp1:MySite/Docs/report.pdf", source_ref=info)
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_resource.return_value = [doc]
            job = _make_job()
            job._reader = MockReader.return_value
            result = job.get_raw_content(item)

        self.assertEqual(result, "")

    def test_page_returns_document_text(self):
        doc = Document(text="Page content", metadata={})
        item = IngestionItem(id="sharepoint:sp1:page", source_ref=doc)
        job = _make_job(sharepoint_type="page")
        self.assertEqual(job.get_raw_content(item), "Page content")

    def test_page_returns_empty_string_for_none_text(self):
        doc = Document(text=None, metadata={})
        item = IngestionItem(id="sharepoint:sp1:page", source_ref=doc)
        job = _make_job(sharepoint_type="page")
        self.assertEqual(job.get_raw_content(item), "")


class TestSharePointIngestionGetItemName(unittest.TestCase):
    def test_drive_safe_name_no_special_chars(self):
        info = _make_info("MySite/Documents/My Report.pdf")
        item = IngestionItem(id="x", source_ref=info)
        job = _make_job()
        name = job.get_item_name(item)
        self.assertNotIn(" ", name)
        self.assertLessEqual(len(name), 255)

    def test_drive_name_truncated_to_255(self):
        info = _make_info("MySite/" + "a" * 300)
        item = IngestionItem(id="x", source_ref=info)
        job = _make_job()
        name = job.get_item_name(item)
        self.assertLessEqual(len(name), 255)

    def test_page_safe_name_no_special_chars(self):
        doc = Document(text="", metadata={"file_path": "Documents/My Page.aspx"})
        item = IngestionItem(id="x", source_ref=doc)
        job = _make_job(sharepoint_type="page")
        name = job.get_item_name(item)
        self.assertNotIn(" ", name)
        self.assertLessEqual(len(name), 255)


class TestSharePointIngestionGetExtraMetadata(unittest.TestCase):
    def test_drive_metadata_fields(self):
        info = {
            "file_path": "MySite/Docs/report.pdf",
            "modified_at": "2024-06-01T12:00:00+00:00",
            "url": "https://sp.example.com/report.pdf",
        }
        item = IngestionItem(id="x", source_ref=info)
        job = _make_job()
        extra = job.get_extra_metadata(item, "content", {})
        self.assertEqual(extra["file_path"], "MySite/Docs/report.pdf")
        self.assertEqual(extra["file_name"], "report.pdf")
        self.assertEqual(extra["url"], "https://sp.example.com/report.pdf")
        self.assertEqual(extra["title"], "report.pdf")

    def test_drive_missing_url_uses_empty_string(self):
        info = {"file_path": "MySite/Docs/report.pdf"}
        item = IngestionItem(id="x", source_ref=info)
        job = _make_job()
        extra = job.get_extra_metadata(item, "content", {})
        self.assertEqual(extra["url"], "")

    def test_page_metadata_fields(self):
        doc = Document(
            text="content",
            metadata={
                "file_path": "pages/home.aspx",
                "file_name": "home.aspx",
                "url": "https://sp.example.com/pages/home.aspx",
                "title": "Home Page",
            },
        )
        item = IngestionItem(id="x", source_ref=doc)
        job = _make_job(sharepoint_type="page")
        extra = job.get_extra_metadata(item, "content", {})
        self.assertEqual(extra["file_path"], "pages/home.aspx")
        self.assertEqual(extra["file_name"], "home.aspx")
        self.assertEqual(extra["url"], "https://sp.example.com/pages/home.aspx")
        self.assertEqual(extra["title"], "Home Page")

    def test_page_title_falls_back_to_file_name(self):
        doc = Document(text="content", metadata={"file_name": "report.aspx"})
        item = IngestionItem(id="x", source_ref=doc)
        job = _make_job(sharepoint_type="page")
        extra = job.get_extra_metadata(item, "content", {})
        self.assertEqual(extra["title"], "report.aspx")

    def test_page_missing_metadata_uses_empty_string(self):
        doc = Document(text="content", metadata={})
        item = IngestionItem(id="x", source_ref=doc)
        job = _make_job(sharepoint_type="page")
        extra = job.get_extra_metadata(item, "content", {})
        self.assertEqual(extra["file_path"], "")
        self.assertEqual(extra["url"], "")


if __name__ == "__main__":
    unittest.main()
