import hashlib
import html
import json
import logging
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from llama_index.core import SimpleDirectoryReader
from llama_index.core.schema import Document
from llama_index.readers.microsoft_sharepoint import SharePointReader, SharePointType
from openpyxl import Workbook

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.helper_classes.ingestion_result import IngestionItemResult, IngestionItemStatus
from tasks.sharepoint_ingestion import SharePointIngestionJob
from utils.http import UrlRedactingFilter


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

    def test_content_format_is_text(self):
        job = _make_job()
        self.assertEqual(job.content_format, "text")

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

    def test_drive_folder_path_without_site_name_raises(self):
        with self.assertRaises(ValueError):
            SharePointIngestionJob(
                {
                    "name": "sp1",
                    "config": {
                        "client_id": "c",
                        "client_secret": "s",
                        "tenant_id": "t",
                        "sharepoint_site_id": "abc-123",
                        "sharepoint_folder_path": "Documents/Reports",
                    },
                }
            )

    def test_drive_folder_path_with_site_name_is_valid(self):
        job = _make_job(sharepoint_folder_path="Documents/Reports")
        self.assertEqual(job.sharepoint_folder_path, "Documents/Reports")


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

    def test_drive_item_id_prefers_file_id(self):
        info = {**_make_info("MySite/Docs/report.pdf"), "file_id": "driveitem-abc123"}
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.list_resources.return_value = [Path("MySite/Docs/report.pdf")]
            MockReader.return_value.get_resource_info.return_value = info
            job = _make_job()
            items = list(job.list_items())

        self.assertIn("driveitem-abc123", items[0].id)
        self.assertNotIn("report.pdf", items[0].id)

    def test_drive_item_id_falls_back_to_path_when_no_file_id(self):
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.list_resources.return_value = [Path("MySite/Docs/report.pdf")]
            MockReader.return_value.get_resource_info.return_value = _make_info("MySite/Docs/report.pdf")
            job = _make_job()
            items = list(job.list_items())

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

    def _drive_content(self, docs):
        item = IngestionItem(id="sharepoint:sp1:MySite/Docs/book.xlsx", source_ref=_make_info("MySite/Docs/book.xlsx"))
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_resource.return_value = docs
            job = _make_job()
            job._reader = MockReader.return_value
            return job.get_raw_content(item)

    def test_drive_joins_all_documents_in_order(self):
        docs = [Document(text="Sheet one", metadata={}), Document(text="Sheet two", metadata={})]
        self.assertEqual(self._drive_content(docs), "Sheet one\n\nSheet two")

    def test_drive_skips_empty_documents(self):
        docs = [
            Document(text="Sheet one", metadata={}),
            Document(text="  \n", metadata={}),
            Document(text=None, metadata={}),
            Document(text="Sheet three", metadata={}),
        ]
        self.assertEqual(self._drive_content(docs), "Sheet one\n\nSheet three")

    def test_drive_returns_empty_when_all_documents_empty(self):
        docs = [Document(text="", metadata={}), Document(text=" ", metadata={})]
        self.assertEqual(self._drive_content(docs), "")

    def test_drive_strips_whitespace_around_single_document(self):
        self.assertEqual(self._drive_content([Document(text="\n File content \n", metadata={})]), "File content")

    def test_drive_keeps_every_sheet_of_real_xlsx(self):
        workbook = Workbook()
        workbook.active.title = "First"
        workbook["First"].append(["header"])
        workbook["First"].append(["alpha_marker"])
        second = workbook.create_sheet("Second")
        second.append(["header"])
        second.append(["beta_marker"])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "book.xlsx"
            workbook.save(path)
            docs = SimpleDirectoryReader(input_files=[str(path)]).load_data()

        result = self._drive_content(docs)

        self.assertIn("alpha_marker", result)
        self.assertIn("beta_marker", result)

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


class TestSharePointIngestionGetItemChecksum(unittest.TestCase):
    def test_drive_returns_etag_when_present(self):
        info = {**_make_info(), "etag": '"{abc123},1"'}
        item = IngestionItem(id="x", source_ref=info)
        job = _make_job()
        self.assertEqual(job.get_item_checksum(item), '"{abc123},1"')

    def test_drive_returns_none_when_etag_absent(self):
        info = _make_info()
        item = IngestionItem(id="x", source_ref=info)
        job = _make_job()
        self.assertIsNone(job.get_item_checksum(item))

    def test_page_returns_none(self):
        doc = Document(text="x", metadata={})
        item = IngestionItem(id="x", source_ref=doc)
        job = _make_job(sharepoint_type="page")
        self.assertIsNone(job.get_item_checksum(item))

    def test_enabled_drive_keeps_etag(self):
        item = IngestionItem(id="x", source_ref={**_make_info("MySite/clip.mp4"), "etag": '"{abc},1"'})
        job = _make_job(transcribe_videos=True)
        self.assertEqual(job.get_item_checksum(item), '"{abc},1"')

    def test_enabled_page_is_md5_of_page_text_and_videos_suffix(self):
        item = IngestionItem(id="x", source_ref=Document(text="Page body", metadata={}))
        with patch("tasks.sharepoint_ingestion.GraphClient"):
            job = _make_job(sharepoint_type="page", transcribe_videos=True)
        expected = hashlib.md5(b"Page body\0transcribe_videos", usedforsecurity=False).hexdigest()
        self.assertEqual(job.get_item_checksum(item), expected)

    def test_enabled_page_checksum_differs_from_the_checksum_of_the_disabled_page(self):
        item = IngestionItem(id="x", source_ref=Document(text="Page body", metadata={}))
        with patch("tasks.sharepoint_ingestion.GraphClient"):
            enabled = _make_job(sharepoint_type="page", transcribe_videos=True)
            disabled = _make_job(sharepoint_type="page", transcribe_videos=False)
        disabled_checksum = hashlib.md5(b"Page body", usedforsecurity=False).hexdigest()
        self.assertIsNone(disabled.get_item_checksum(item))
        self.assertNotEqual(enabled.get_item_checksum(item), disabled_checksum)

    def test_enabled_page_checksum_ignores_video_work(self):
        item = IngestionItem(id="x", source_ref=Document(text="Page body", metadata={}))
        with patch("tasks.sharepoint_ingestion.GraphClient") as MockGraph:
            job = _make_job(sharepoint_type="page", transcribe_videos=True)
            job._reader = MagicMock()
            job.get_item_checksum(item)
        job._reader.get_page_text.assert_not_called()
        MockGraph.return_value.get_item_by_web_url.assert_not_called()


class TestSharePointVideoConfig(unittest.TestCase):
    def test_defaults(self):
        job = _make_job()
        self.assertFalse(job.transcribe_videos)
        self.assertEqual(job.max_video_bytes, 500 * 1024 * 1024)

    def test_transcribe_videos_string_true(self):
        self.assertTrue(_make_job(transcribe_videos="true").transcribe_videos)

    def test_max_video_mb_converted_to_bytes(self):
        job = _make_job(transcribe_videos=True, max_video_mb="1.5")
        self.assertEqual(job.max_video_bytes, int(1.5 * 1024 * 1024))

    def test_invalid_max_video_mb_raises_when_enabled(self):
        for value in (True, 0, -5, "abc", None, float("inf"), float("nan")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                _make_job(transcribe_videos=True, max_video_mb=value)

    def test_invalid_max_video_mb_is_ignored_when_disabled(self):
        for value in (True, 0, -5, "abc", None, float("inf"), float("nan")):
            with self.subTest(value=value):
                job = _make_job(max_video_mb=value)
                self.assertFalse(job.transcribe_videos)
                self.assertEqual(job.max_video_bytes, 500 * 1024 * 1024)

    def test_graph_client_built_once_for_page_mode_with_cap(self):
        with patch("tasks.sharepoint_ingestion.GraphClient") as MockGraph:
            job = _make_job(sharepoint_type="page", transcribe_videos=True, max_video_mb=2)
        MockGraph.assert_called_once_with(
            "test-client-id", "test-client-secret", "test-tenant-id", max_file_size_bytes=2 * 1024 * 1024
        )
        self.assertIs(job._graph, MockGraph.return_value)

    def test_graph_client_not_built_when_disabled_or_file_mode(self):
        with patch("tasks.sharepoint_ingestion.GraphClient") as MockGraph:
            _make_job(sharepoint_type="page")
            _make_job(transcribe_videos=True)
        MockGraph.assert_not_called()


class TestSharePointSegmentPrefixRemoval(unittest.TestCase):
    def test_removes_one_prefix_per_segment(self):
        text = "[00:00-00:04] Hello there.\n\n[00:04-00:09] Second part."
        self.assertEqual(SharePointIngestionJob._strip_segment_prefixes(text), "Hello there.\n\nSecond part.")

    def test_removes_hour_prefix_and_mixed_endpoints(self):
        text = "[59:58-1:00:03] Crosses the hour.\n\n[1:02:03-1:02:09] Later."
        self.assertEqual(SharePointIngestionJob._strip_segment_prefixes(text), "Crosses the hour.\n\nLater.")

    def test_keeps_inline_lookalikes_and_spoken_times(self):
        text = "[00:00-00:05] The meeting is at 3:30 on June 5 and [01:00-01:05] is not a prefix."
        self.assertEqual(
            SharePointIngestionJob._strip_segment_prefixes(text),
            "The meeting is at 3:30 on June 5 and [01:00-01:05] is not a prefix.",
        )

    def test_removes_only_the_first_prefix_of_a_segment(self):
        text = "[00:00-00:05] [00:05-00:09] Spoken lookalike."
        self.assertEqual(SharePointIngestionJob._strip_segment_prefixes(text), "[00:05-00:09] Spoken lookalike.")

    def test_text_without_prefix_is_unchanged(self):
        self.assertEqual(SharePointIngestionJob._strip_segment_prefixes("Plain speech."), "Plain speech.")


def _video_job(**cfg):
    job = _make_job(transcribe_videos=True, **cfg)
    job._reader = MagicMock()
    return job


class TestSharePointFileVideo(unittest.TestCase):
    def _item(self, file_path="MySite/Documents/clip.mp4", **info):
        return IngestionItem(id="sp:clip", source_ref={**_make_info(file_path), **info})

    def test_every_video_extension_is_transcribed_case_insensitively(self):
        for ext in (".mp4", ".avi", ".mov", ".mkv", ".webm", ".MP4", ".WebM"):
            with self.subTest(ext=ext):
                job = _video_job()
                job._reader.read_file_content.return_value = b"video"
                with patch.object(
                    SharePointIngestionJob, "convert_video_to_markdown", return_value="[00:00-00:03] Hi."
                ) as convert:
                    result = job.get_raw_content(self._item(f"MySite/Documents/clip{ext}"))
                self.assertEqual(result, IngestionItemResult(IngestionItemStatus.SUCCESS, "Hi."))
                self.assertEqual(Path(convert.call_args[0][0]).suffix, ext.lower())
                job._reader.load_resource.assert_not_called()

    def test_non_video_file_uses_load_resource(self):
        job = _video_job()
        job._reader.load_resource.return_value = [_make_doc(text="PDF text")]
        self.assertEqual(job.get_raw_content(self._item("MySite/Documents/report.pdf")), "PDF text")
        job._reader.read_file_content.assert_not_called()

    def test_disabled_video_keeps_the_old_path(self):
        job = _make_job()
        job._reader = MagicMock()
        job._reader.load_resource.return_value = [_make_doc(text="")]
        self.assertEqual(job.get_raw_content(self._item()), "")
        job._reader.read_file_content.assert_not_called()

    def test_declared_oversize_is_not_downloaded(self):
        job = _video_job(max_video_mb=1)
        result = job.get_raw_content(self._item(size=1024 * 1024 + 1))
        self.assertEqual(result.status, IngestionItemStatus.FAIL)
        job._reader.read_file_content.assert_not_called()

    def test_actual_oversize_fails_when_size_is_missing_or_false(self):
        for info in ({}, {"size": 1}, {"size": "big"}):
            with self.subTest(info=info):
                job = _video_job(max_video_mb=0.001)
                job._reader.read_file_content.return_value = b"x" * 1100
                with patch.object(SharePointIngestionJob, "convert_video_to_markdown") as convert:
                    result = job.get_raw_content(self._item(**info))
                self.assertEqual(result.status, IngestionItemStatus.FAIL)
                convert.assert_not_called()

    def test_file_exactly_at_the_cap_is_converted(self):
        job = _video_job(max_video_mb=0.001)
        cap = job.max_video_bytes
        job._reader.read_file_content.return_value = b"x" * cap
        with patch.object(SharePointIngestionJob, "convert_video_to_markdown", return_value="Speech."):
            result = job.get_raw_content(self._item(size=cap))
        self.assertEqual(result.status, IngestionItemStatus.SUCCESS)

    def test_reader_error_log_does_not_leak_the_signed_url(self):
        _video_job()
        reader_logger = logging.getLogger(SharePointReader.__module__)
        with self.assertLogs(reader_logger, level="ERROR") as logs:
            reader_logger.error(
                "An error occurred while reading file content from SharePoint: %s",
                ConnectionError("Max retries exceeded with url: /f.mp4?tempauth=SECRET (Caused by X)"),
            )
        self.assertNotIn("SECRET", "".join(logs.output))
        self.assertIn("reading file content", "".join(logs.output))

    def test_reader_log_filter_is_added_once_and_only_when_enabled(self):
        reader_logger = logging.getLogger(SharePointReader.__module__)
        for existing in [f for f in reader_logger.filters if isinstance(f, UrlRedactingFilter)]:
            reader_logger.removeFilter(existing)
        _make_job()
        self.assertEqual([f for f in reader_logger.filters if isinstance(f, UrlRedactingFilter)], [])
        _video_job()
        _video_job()
        self.assertEqual(len([f for f in reader_logger.filters if isinstance(f, UrlRedactingFilter)]), 1)

    def test_download_error_fails_without_leaking_the_signed_url(self):
        job = _video_job()
        job._reader.read_file_content.side_effect = ConnectionError("GET https://x.sharepoint.com/f?tempauth=SECRET")
        with self.assertLogs("tasks.sharepoint_ingestion", level="WARNING") as logs:
            result = job.get_raw_content(self._item())
        self.assertEqual(result.status, IngestionItemStatus.FAIL)
        self.assertNotIn("SECRET", "".join(logs.output) + "".join(result.errors))

    def test_converter_error_fails(self):
        job = _video_job()
        job._reader.read_file_content.return_value = b"video"
        with patch.object(SharePointIngestionJob, "convert_video_to_markdown", side_effect=RuntimeError("partial")):
            result = job.get_raw_content(self._item())
        self.assertEqual(result.status, IngestionItemStatus.FAIL)

    def test_video_without_speech_is_an_explicit_empty_success(self):
        for transcript in ("", "   ", "[00:00-00:03] "):
            with self.subTest(transcript=transcript):
                job = _video_job()
                job._reader.read_file_content.return_value = b"video"
                with patch.object(SharePointIngestionJob, "convert_video_to_markdown", return_value=transcript):
                    result = job.get_raw_content(self._item())
                self.assertEqual(result, IngestionItemResult(IngestionItemStatus.SUCCESS, ""))

    def test_video_without_speech_is_recorded_once_and_not_transcribed_again(self):
        job = _video_job()
        job.metadata_tracker = MagicMock()
        job.vector_manager = MagicMock()
        job.metadata_tracker.get_latest_record.return_value = None
        job._reader.read_file_content.return_value = b"video"
        item = self._item(etag='"{abc},1"')
        with patch.object(SharePointIngestionJob, "convert_video_to_markdown", return_value="") as convert:
            job.process_item(item)
            job.vector_manager.insert_documents.assert_not_called()
            job.metadata_tracker.record_metadata.assert_called_once()
            self.assertEqual(job.metadata_tracker.record_metadata.call_args.args[1], '"{abc},1"')

            next_run = _video_job()
            next_run.metadata_tracker = MagicMock()
            next_run.vector_manager = MagicMock()
            next_run.metadata_tracker.get_latest_record.return_value = MagicMock(checksum='"{abc},1"', version=1)
            next_run.process_item(item)

        convert.assert_called_once()
        job._reader.read_file_content.assert_called_once()
        next_run._reader.read_file_content.assert_not_called()
        next_run.metadata_tracker.record_metadata.assert_not_called()

    def test_temp_directory_is_removed_after_success_and_failure(self):
        for side_effect in (None, RuntimeError("boom")):
            with self.subTest(failing=side_effect is not None):
                seen = []

                def convert(path, _side_effect=side_effect, _seen=seen):
                    _seen.append(Path(path))
                    self.assertTrue(Path(path).exists())
                    if _side_effect:
                        raise _side_effect
                    return "Speech."

                job = _video_job()
                job._reader.read_file_content.return_value = b"video"
                with patch.object(SharePointIngestionJob, "convert_video_to_markdown", side_effect=convert):
                    job.get_raw_content(self._item())
                self.assertFalse(seen[0].parent.exists())

    def test_unchanged_video_is_skipped_by_etag_before_download(self):
        job = _video_job()
        job.metadata_tracker = MagicMock()
        job.vector_manager = MagicMock()
        job.metadata_tracker.get_latest_record.return_value = MagicMock(checksum='"{abc},1"', version=1)
        job.process_item(self._item(etag='"{abc},1"'))
        job._reader.read_file_content.assert_not_called()
        job._reader.load_resource.assert_not_called()


def _canvas_html(*webparts):
    """Build CanvasContent1-like HTML with one ``data-sp-webpartdata`` attribute per web part."""
    return "".join(
        f'<div data-sp-webpart="" data-sp-webpartdata="{html.escape(json.dumps(part))}"></div>' for part in webparts
    )


def _file_part(url):
    return {"title": "File viewer", "properties": {"file": url}, "serverProcessedContent": {"links": {"file": url}}}


SITE_ID = "contoso.sharepoint.com,11111111-aaaa,22222222-bbbb"


def _drive_item(name, drive_id="d1", item_id="i1", size=1000, web_url=None):
    return {
        "id": item_id,
        "name": name,
        "size": size,
        "webUrl": web_url or f"https://contoso.sharepoint.com/sites/s/Shared%20Documents/{name}",
        "parentReference": {"driveId": drive_id},
        "@microsoft.graph.downloadUrl": f"https://download.example/{item_id}?tempauth=SECRET",
    }


class TestSharePointGraphClientLifecycle(unittest.TestCase):
    def _page_job(self):
        with patch("tasks.sharepoint_ingestion.GraphClient") as mock_graph:
            job = _make_job(sharepoint_type="page", transcribe_videos=True)
        return job, mock_graph.return_value

    def test_graph_client_is_closed_after_the_listing_ends(self):
        job, graph = self._page_job()
        with (
            patch("tasks.sharepoint_ingestion.SharePointReader"),
            patch.object(job, "_list_page_items", return_value=iter([])),
        ):
            list(job.list_items())
        graph.close.assert_called_once_with()

    def test_graph_client_is_closed_when_the_listing_is_abandoned(self):
        job, graph = self._page_job()
        with (
            patch("tasks.sharepoint_ingestion.SharePointReader"),
            patch.object(job, "_list_page_items", return_value=iter([IngestionItem(id="p", source_ref=None)])),
        ):
            listing = job.list_items()
            next(listing)
            listing.close()
        graph.close.assert_called_once_with()

    def test_listing_without_a_graph_client_does_not_fail(self):
        job = _make_job(sharepoint_type="page")
        with (
            patch("tasks.sharepoint_ingestion.SharePointReader"),
            patch.object(job, "_list_page_items", return_value=iter([])),
        ):
            self.assertEqual(list(job.list_items()), [])


class TestSharePointPageVideos(unittest.TestCase):
    def _job(self, raw_html, items_by_url=None, **cfg):
        with patch("tasks.sharepoint_ingestion.GraphClient") as MockGraph:
            job = _make_job(sharepoint_type="page", transcribe_videos=True, **cfg)
        job._reader = MagicMock()
        job._reader.get_page_text.return_value = {"rawHtml": raw_html}
        job._graph.get_item_by_web_url.side_effect = lambda url: items_by_url[url]
        job._graph.download_content.side_effect = lambda url: url.encode()
        self.assertIs(job._graph, MockGraph.return_value)
        return job

    def _doc(self, text="Page text."):
        return Document(text=text, metadata={"site_id": SITE_ID, "page_id": "list1:7"}, id_="list1:7")

    @staticmethod
    def _convert(path):
        return f"[00:00-00:05] Speech of {Path(path).read_bytes().decode().split('/')[-1].split('?')[0]}."

    def test_appends_transcripts_in_page_order_with_labels(self):
        urls = [
            "https://contoso.sharepoint.com/sites/s/Shared%20Documents/a.mp4",
            "https://contoso.sharepoint.com/sites/s/Shared%20Documents/b.webm",
        ]
        job = self._job(
            _canvas_html(_file_part(urls[0]), _file_part(urls[1])),
            {urls[0]: _drive_item("a.mp4", item_id="i1"), urls[1]: _drive_item("b.webm", item_id="i2")},
        )
        with patch.object(SharePointIngestionJob, "convert_video_to_markdown", side_effect=self._convert):
            text = job.get_raw_content(IngestionItem(id="p", source_ref=self._doc()))
        self.assertTrue(text.startswith("Page text.\n\n## Video transcript: a.mp4\n\nhttps://contoso.sharepoint.com/"))
        self.assertLess(text.index("a.mp4"), text.index("b.webm"))
        self.assertIn("Speech of i1.", text)
        self.assertIn("Speech of i2.", text)
        self.assertNotIn("[00:00-00:05]", text)

    def test_same_video_through_two_references_is_converted_once(self):
        urls = [
            "https://contoso.sharepoint.com/sites/s/Shared%20Documents/a.mp4",
            "https://contoso.sharepoint.com/sites/s/Other/a.mp4",
        ]
        job = self._job(
            _canvas_html(_file_part(urls[0]), _file_part(urls[1])),
            {url: _drive_item("a.mp4") for url in urls},
        )
        with patch.object(SharePointIngestionJob, "convert_video_to_markdown", return_value="Speech.") as convert:
            job.get_raw_content(IngestionItem(id="p", source_ref=self._doc()))
        convert.assert_called_once()

    def test_external_and_other_tenant_videos_are_not_resolved_or_downloaded(self):
        html_page = _canvas_html(
            _file_part("https://www.youtube.com/watch?v=abc.mp4"),
            _file_part("https://other.sharepoint.com/sites/s/a.mp4"),
        )
        job = self._job(html_page, {})
        text = job.get_raw_content(IngestionItem(id="p", source_ref=self._doc()))
        self.assertEqual(text, "Page text.")
        job._graph.get_item_by_web_url.assert_not_called()
        job._graph.download_content.assert_not_called()

    def test_failed_video_does_not_block_page_text_or_the_next_video(self):
        urls = [
            "https://contoso.sharepoint.com/sites/s/Shared%20Documents/a.mp4",
            "https://contoso.sharepoint.com/sites/s/Shared%20Documents/b.mp4",
        ]
        job = self._job(
            _canvas_html(_file_part(urls[0]), _file_part(urls[1])),
            {urls[0]: _drive_item("a.mp4", item_id="i1"), urls[1]: _drive_item("b.mp4", item_id="i2")},
        )
        job._graph.download_content.side_effect = [
            RuntimeError("Download failed (403) for https://download.example/i1?tempauth=SECRET"),
            b"video",
        ]
        with patch.object(SharePointIngestionJob, "convert_video_to_markdown", return_value="Second speech."):
            with self.assertLogs("tasks.sharepoint_ingestion", level="WARNING") as logs:
                text = job.get_raw_content(IngestionItem(id="p", source_ref=self._doc()))
        self.assertIn("Page text.", text)
        self.assertIn("Second speech.", text)
        self.assertNotIn("a.mp4\n\n", text)
        self.assertNotIn("SECRET", "".join(logs.output))

    def test_unresolvable_reference_does_not_stop_later_videos(self):
        urls = [
            "https://contoso.sharepoint.com/sites/s/Shared%20Documents/gone.mp4",
            "https://contoso.sharepoint.com/sites/s/Shared%20Documents/b.mp4",
        ]
        job = self._job(_canvas_html(_file_part(urls[0]), _file_part(urls[1])), {urls[1]: _drive_item("b.mp4")})
        with patch.object(SharePointIngestionJob, "convert_video_to_markdown", return_value="Speech."):
            text = job.get_raw_content(IngestionItem(id="p", source_ref=self._doc()))
        self.assertIn("## Video transcript: b.mp4", text)

    def test_oversize_and_non_video_items_are_skipped_without_download(self):
        urls = [
            "https://contoso.sharepoint.com/sites/s/Shared%20Documents/big.mp4",
            "https://contoso.sharepoint.com/sites/s/Shared%20Documents/renamed.mp4",
        ]
        job = self._job(
            _canvas_html(_file_part(urls[0]), _file_part(urls[1])),
            {urls[0]: _drive_item("big.mp4", size=3 * 1024 * 1024), urls[1]: _drive_item("notes.docx", item_id="i2")},
            max_video_mb=1,
        )
        text = job.get_raw_content(IngestionItem(id="p", source_ref=self._doc()))
        self.assertEqual(text, "Page text.")
        job._graph.download_content.assert_not_called()

    def test_page_html_fetch_failure_keeps_the_page_text(self):
        job = self._job("", {})
        job._reader.get_page_text.side_effect = RuntimeError("Graph down")
        text = job.get_raw_content(IngestionItem(id="p", source_ref=self._doc()))
        self.assertEqual(text, "Page text.")

    def test_page_without_ids_keeps_the_page_text(self):
        job = self._job("", {})
        doc = Document(text="Page text.", metadata={}, id_="")
        self.assertEqual(job.get_raw_content(IngestionItem(id="p", source_ref=doc)), "Page text.")
        job._reader.get_page_text.assert_not_called()

    def test_silent_video_adds_nothing(self):
        url = "https://contoso.sharepoint.com/sites/s/Shared%20Documents/a.mp4"
        job = self._job(_canvas_html(_file_part(url)), {url: _drive_item("a.mp4")})
        with patch.object(SharePointIngestionJob, "convert_video_to_markdown", return_value=""):
            text = job.get_raw_content(IngestionItem(id="p", source_ref=self._doc()))
        self.assertEqual(text, "Page text.")

    def test_text_less_page_with_only_a_silent_video_is_an_explicit_empty_success(self):
        url = "https://contoso.sharepoint.com/sites/s/Shared%20Documents/a.mp4"
        job = self._job(_canvas_html(_file_part(url)), {url: _drive_item("a.mp4")})
        with patch.object(SharePointIngestionJob, "convert_video_to_markdown", return_value=""):
            result = job.get_raw_content(IngestionItem(id="p", source_ref=self._doc(text="")))
        self.assertEqual(result, IngestionItemResult(IngestionItemStatus.SUCCESS, ""))

    def test_text_less_page_stays_a_retryable_empty_string_when_a_video_failed_or_none_found(self):
        url = "https://contoso.sharepoint.com/sites/s/Shared%20Documents/a.mp4"
        failing = self._job(_canvas_html(_file_part(url)), {url: _drive_item("a.mp4")})
        failing._graph.download_content.side_effect = RuntimeError("boom")
        no_videos = self._job("", {})
        with patch.object(SharePointIngestionJob, "convert_video_to_markdown", return_value=""):
            self.assertEqual(failing.get_raw_content(IngestionItem(id="p", source_ref=self._doc(text=""))), "")
            self.assertEqual(no_videos.get_raw_content(IngestionItem(id="p", source_ref=self._doc(text=""))), "")

    def test_unchanged_page_does_no_video_work(self):
        url = "https://contoso.sharepoint.com/sites/s/Shared%20Documents/a.mp4"
        job = self._job(_canvas_html(_file_part(url)), {url: _drive_item("a.mp4")})
        job.metadata_tracker = MagicMock()
        job.vector_manager = MagicMock()
        checksum = hashlib.md5(b"Page text.\0transcribe_videos", usedforsecurity=False).hexdigest()
        job.metadata_tracker.get_latest_record.return_value = MagicMock(checksum=checksum, version=1)
        with patch.object(SharePointIngestionJob, "convert_video_to_markdown") as convert:
            job.process_item(IngestionItem(id="p", source_ref=self._doc()))
        job._reader.get_page_text.assert_not_called()
        job._graph.get_item_by_web_url.assert_not_called()
        job._graph.download_content.assert_not_called()
        convert.assert_not_called()

    def test_turning_videos_on_ingests_a_page_stored_without_transcripts(self):
        url = "https://contoso.sharepoint.com/sites/s/Shared%20Documents/a.mp4"
        job = self._job(_canvas_html(_file_part(url)), {url: _drive_item("a.mp4")})
        job.metadata_tracker = MagicMock()
        job.vector_manager = MagicMock()
        stored = hashlib.md5(b"Page text.", usedforsecurity=False).hexdigest()
        job.metadata_tracker.get_latest_record.return_value = MagicMock(checksum=stored, version=1)
        with patch.object(SharePointIngestionJob, "convert_video_to_markdown", return_value="Speech."):
            job.process_item(IngestionItem(id="p", source_ref=self._doc()))
        stored_doc = job.vector_manager.insert_documents.call_args[0][0][0]
        self.assertIn("Speech.", stored_doc.text)

    def test_turning_videos_off_ingests_a_page_stored_with_transcripts(self):
        with patch("tasks.sharepoint_ingestion.GraphClient"):
            job = _make_job(sharepoint_type="page", transcribe_videos=False)
        job.metadata_tracker = MagicMock()
        job.vector_manager = MagicMock()
        stored = hashlib.md5(b"Page text.\0transcribe_videos", usedforsecurity=False).hexdigest()
        job.metadata_tracker.get_latest_record.return_value = MagicMock(checksum=stored, version=1)
        job.process_item(IngestionItem(id="p", source_ref=self._doc()))
        stored_doc = job.vector_manager.insert_documents.call_args[0][0][0]
        self.assertEqual(stored_doc.text, "Page text.")

    def test_changed_page_stores_the_checksum_of_the_original_text(self):
        url = "https://contoso.sharepoint.com/sites/s/Shared%20Documents/a.mp4"
        job = self._job(_canvas_html(_file_part(url)), {url: _drive_item("a.mp4")})
        job.metadata_tracker = MagicMock()
        job.vector_manager = MagicMock()
        job.metadata_tracker.get_latest_record.return_value = None
        with patch.object(SharePointIngestionJob, "convert_video_to_markdown", return_value="Speech."):
            job.process_item(IngestionItem(id="p", source_ref=self._doc()))
        stored_doc = job.vector_manager.insert_documents.call_args[0][0][0]
        self.assertIn("Speech.", stored_doc.text)
        expected = hashlib.md5(b"Page text.\0transcribe_videos", usedforsecurity=False).hexdigest()
        self.assertEqual(stored_doc.metadata["checksum"], expected)


if __name__ == "__main__":
    unittest.main()
