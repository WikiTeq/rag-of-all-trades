import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from llama_index.core import SimpleDirectoryReader
from llama_index.core.schema import Document
from llama_index.readers.microsoft_sharepoint import SharePointType
from openpyxl import Workbook

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.helper_classes.ingestion_result import IngestionItemStatus
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

    def test_content_format_is_text_for_drive(self):
        job = _make_job()
        self.assertEqual(job.content_format, "text")

    def test_content_format_is_markdown_for_page(self):
        job = _make_job(sharepoint_type="page")
        self.assertEqual(job.content_format, "markdown")

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

    def test_site_id_only_config_calls_load_data_without_unsupported_keyword(self):
        with patch("tasks.sharepoint_ingestion.SharePointReader", autospec=True) as MockReader:
            MockReader.return_value.load_data.return_value = []
            job = _make_job(sharepoint_type="page", sharepoint_site_name="", sharepoint_site_id="abc-123")
            list(job.list_items())

        self.assertEqual(MockReader.call_args[1]["sharepoint_site_id"], "abc-123")
        MockReader.return_value.load_data.assert_called_once_with()

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

    @staticmethod
    def _page_item(site_id="site-1", page_id="list-1:42", text="reader text with JSON"):
        metadata = {}
        if site_id is not None:
            metadata["site_id"] = site_id
        if page_id is not None:
            metadata["page_id"] = page_id
        doc_kwargs = {"id_": page_id} if page_id is not None else {}
        return IngestionItem(id="sharepoint:sp1:page", source_ref=Document(text=text, metadata=metadata, **doc_kwargs))

    def _page_content(self, item, page_text=None, side_effect=None):
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            job = _make_job(sharepoint_type="page")
            job._reader = MockReader.return_value
            if side_effect is not None:
                job._reader.get_page_text.side_effect = side_effect
            else:
                job._reader.get_page_text.return_value = page_text
            return job, job.get_raw_content(item)

    def test_page_fetches_html_with_ids_from_metadata(self):
        job, _ = self._page_content(self._page_item(), {"rawHtml": "<p>Hi</p>"})
        job._reader.get_page_text.assert_called_once_with(site_id="site-1", list_id="list-1", page_id="list-1:42")

    def test_page_converts_html_to_markdown(self):
        html = "<h2>Heading</h2><p>Real text</p><table><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        _, result = self._page_content(self._page_item(), {"rawHtml": html})
        self.assertEqual(result.status, IngestionItemStatus.SUCCESS)
        self.assertIn("## Heading", result.result)
        self.assertIn("Real text", result.result)
        self.assertIn("| A |", result.result)

    def test_page_drops_escaped_web_part_json(self):
        html = (
            '<div data-sp-webpartdata="{&quot;titleHTML&quot;:&quot;&lt;b&gt;Learn more&lt;/b&gt;'
            '&quot;,&quot;image&quot;:{&quot;siteId&quot;:&quot;x&quot;}}"><p>Real text here</p></div>'
        )
        _, result = self._page_content(self._page_item(), {"rawHtml": html})
        self.assertIn("Real text here", result.result)
        self.assertNotIn("siteId", result.result)
        self.assertNotIn("titleHTML", result.result)

    def test_page_does_not_use_reader_text(self):
        _, result = self._page_content(self._page_item(text="reader text with JSON"), {"rawHtml": "<p>Clean</p>"})
        self.assertNotIn("reader text with JSON", result.result)

    def test_page_fetch_error_returns_fail(self):
        _, result = self._page_content(self._page_item(), side_effect=RuntimeError("boom"))
        self.assertEqual(result.status, IngestionItemStatus.FAIL)
        self.assertIn("boom", result.errors[0])

    def test_page_blank_html_returns_empty_success(self):
        for raw_html in (None, "", "   "):
            with self.subTest(raw_html=raw_html):
                _, result = self._page_content(self._page_item(), {"rawHtml": raw_html})
                self.assertEqual(result.status, IngestionItemStatus.SUCCESS)
                self.assertEqual(result.result, "")

    def test_page_empty_conversion_returns_fail(self):
        html = '<div data-sp-webpartdata="{&quot;title&quot;:&quot;only attribute&quot;}"></div>'
        _, result = self._page_content(self._page_item(), {"rawHtml": html})
        self.assertEqual(result.status, IngestionItemStatus.FAIL)

    def test_page_conversion_exception_returns_fail(self):
        with patch.object(SharePointIngestionJob, "_get_markitdown") as get_markitdown:
            get_markitdown.return_value.convert_stream.side_effect = ValueError("bad html")
            _, result = self._page_content(self._page_item(), {"rawHtml": "<p>Hi</p>"})
        self.assertEqual(result.status, IngestionItemStatus.FAIL)

    def test_page_missing_or_malformed_ids_return_fail_without_request(self):
        cases = [
            self._page_item(site_id=None),
            self._page_item(page_id=None),
            self._page_item(page_id="no-colon"),
        ]
        for item in cases:
            with self.subTest(metadata=item.source_ref.metadata):
                job, result = self._page_content(item, {"rawHtml": "<p>Hi</p>"})
                self.assertEqual(result.status, IngestionItemStatus.FAIL)
                job._reader.get_page_text.assert_not_called()

    def test_drive_makes_no_page_request(self):
        with patch("tasks.sharepoint_ingestion.SharePointReader") as MockReader:
            MockReader.return_value.load_resource.return_value = [_make_doc(text="x")]
            job = _make_job()
            job._reader = MockReader.return_value
            job.get_raw_content(IngestionItem(id="i", source_ref={"file_path": "a.pdf"}))
        MockReader.return_value.get_page_text.assert_not_called()


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


if __name__ == "__main__":
    unittest.main()
