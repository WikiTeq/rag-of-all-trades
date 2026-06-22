import unittest
from datetime import UTC, datetime
from unittest.mock import Mock, patch

from tasks.confluence_ingestion import ConfluenceIngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem


def _make_config(
    base_url="https://example.atlassian.net/wiki",
    api_token="token123",
    username="user@example.com",
    password=None,
    cloud=True,
    space_key="ENG",
    page_ids=None,
    page_label=None,
    cql=None,
    folder_id=None,
    page_status=None,
    include_children=False,
    max_pages=50,
):
    cfg = {
        "base_url": base_url,
        "cloud": cloud,
        "max_pages": max_pages,
        "include_children": include_children,
    }
    if api_token:
        cfg["api_token"] = api_token
    if username:
        cfg["username"] = username
    if password:
        cfg["password"] = password
    if space_key:
        cfg["space_key"] = space_key
    if page_ids:
        cfg["page_ids"] = page_ids
    if page_label:
        cfg["page_label"] = page_label
    if cql:
        cfg["cql"] = cql
    if folder_id:
        cfg["folder_id"] = folder_id
    if page_status:
        cfg["page_status"] = page_status
    return {"name": "test_confluence", "config": cfg}


def _make_doc(
    page_id="123",
    title="My Page",
    url="https://example.com/page",
    space_key="ENG",
    text="Page content",
):
    doc = Mock()
    doc.text = text
    doc.metadata = {
        "page_id": page_id,
        "title": title,
        "url": url,
        "space_key": space_key,
    }
    return doc


class TestConfluenceIngestionJob(unittest.TestCase):
    def setUp(self):
        self.reader_patcher = patch("tasks.confluence_ingestion.ConfluenceReader")
        self.mock_reader_class = self.reader_patcher.start()
        self.mock_reader = Mock()
        self.mock_reader_class.return_value = self.mock_reader
        self.mock_reader.confluence.get_page_by_id.return_value = {
            "version": {"when": "2024-01-01T00:00:00.000Z"},
            "space": {"key": "ENG"},
        }

    def tearDown(self):
        self.reader_patcher.stop()

    def _make_job(self, **kwargs):
        return ConfluenceIngestionJob(_make_config(**kwargs))

    def test_missing_base_url_raises(self):
        with self.assertRaises(ValueError):
            ConfluenceIngestionJob({"name": "x", "config": {"api_token": "t", "space_key": "ENG"}})

    def test_no_auth_raises(self):
        with self.assertRaises(ValueError):
            ConfluenceIngestionJob(
                {
                    "name": "x",
                    "config": {
                        "base_url": "https://x.atlassian.net/wiki",
                        "space_key": "ENG",
                    },
                }
            )

    def test_api_token_and_password_mutually_exclusive(self):
        with self.assertRaises(ValueError):
            self._make_job(api_token="tok", password="pass")

    def test_password_without_username_raises(self):
        with self.assertRaises(ValueError):
            self._make_job(api_token=None, username=None, password="secret")

    def test_no_discovery_mode_raises(self):
        with self.assertRaises(ValueError):
            ConfluenceIngestionJob(
                {
                    "name": "x",
                    "config": {
                        "base_url": "https://x.atlassian.net/wiki",
                        "api_token": "t",
                    },
                }
            )

    def test_multiple_discovery_modes_raises(self):
        with self.assertRaises(ValueError):
            self._make_job(space_key="ENG", cql="type=page")

    def test_non_positive_max_pages_raises(self):
        with self.assertRaises(ValueError):
            self._make_job(max_pages=0)

    def test_reader_constructed_with_api_token_and_username_uses_basic_auth(self):
        self._make_job(api_token="mytoken", username="user@example.com")
        self.mock_reader_class.assert_called_once_with(
            base_url="https://example.atlassian.net/wiki",
            cloud=True,
            user_name="user@example.com",
            password="mytoken",
        )

    def test_reader_constructed_with_password_uses_basic_auth(self):
        self._make_job(api_token=None, username="admin", password="secret", space_key="ENG")
        self.mock_reader_class.assert_called_once_with(
            base_url="https://example.atlassian.net/wiki",
            cloud=True,
            user_name="admin",
            password="secret",
        )

    def test_reader_constructed_with_api_token_only_uses_bearer(self):
        self._make_job(api_token="myPAT", username=None, cloud=False)
        self.mock_reader_class.assert_called_once_with(
            base_url="https://example.atlassian.net/wiki",
            cloud=False,
            api_token="myPAT",
        )

    def test_trailing_slash_stripped_from_base_url(self):
        self._make_job(base_url="https://example.atlassian.net/wiki/")
        call_kwargs = self.mock_reader_class.call_args.kwargs
        self.assertEqual(call_kwargs["base_url"], "https://example.atlassian.net/wiki")

    def test_list_items_yields_ingestion_items(self):
        doc1 = _make_doc(page_id="1", title="Page One")
        doc2 = _make_doc(page_id="2", title="Page Two")
        self.mock_reader.load_data.return_value = [doc1, doc2]

        job = self._make_job()
        items = list(job.list_items())

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].id, "confluence:1")
        self.assertEqual(items[1].id, "confluence:2")
        self.assertIsInstance(items[0], IngestionItem)
        self.assertIs(items[0].source_ref, doc1)
        expected_ts = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        self.assertEqual(items[0].last_modified, expected_ts)
        self.assertEqual(job.get_item_checksum(items[0]), expected_ts.isoformat())

    def test_list_items_skips_doc_with_no_page_id(self):
        doc = _make_doc(page_id="1", title="Good")
        doc_no_id = Mock()
        doc_no_id.text = "content"
        doc_no_id.metadata = {"title": "No ID page"}
        self.mock_reader.load_data.return_value = [doc_no_id, doc]

        job = self._make_job()
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "confluence:1")

    def test_list_items_empty_result(self):
        self.mock_reader.load_data.return_value = []
        job = self._make_job()
        self.assertEqual(list(job.list_items()), [])

    def test_list_items_reader_error_yields_nothing(self):
        self.mock_reader.load_data.side_effect = Exception("API error")
        job = self._make_job()
        self.assertEqual(list(job.list_items()), [])

    def test_space_key_mode_passes_correct_kwargs(self):
        self.mock_reader.load_data.return_value = []
        job = self._make_job(space_key="ENG", page_status="current")
        list(job.list_items())
        self.mock_reader.load_data.assert_called_once_with(max_num_results=50, space_key="ENG", page_status="current")

    def test_page_ids_mode_passes_correct_kwargs(self):
        self.mock_reader.load_data.return_value = []
        job = self._make_job(space_key=None, page_ids="111,222", include_children=True)
        list(job.list_items())
        self.mock_reader.load_data.assert_called_once_with(
            max_num_results=50, page_ids=["111", "222"], include_children=True
        )

    def test_label_mode_passes_correct_kwargs(self):
        self.mock_reader.load_data.return_value = []
        job = self._make_job(space_key=None, page_label="my-label")
        list(job.list_items())
        self.mock_reader.load_data.assert_called_once_with(max_num_results=50, label="my-label")

    def test_cql_mode_passes_correct_kwargs(self):
        self.mock_reader.load_data.return_value = []
        job = self._make_job(space_key=None, cql="space = 'TEST'")
        list(job.list_items())
        self.mock_reader.load_data.assert_called_once_with(max_num_results=50, cql="space = 'TEST'")

    def test_folder_id_mode_passes_correct_kwargs(self):
        self.mock_reader.load_data.return_value = []
        job = self._make_job(space_key=None, folder_id="12345")
        list(job.list_items())
        self.mock_reader.load_data.assert_called_once_with(max_num_results=50, folder_id="12345")

    def test_get_raw_content_returns_doc_text(self):
        doc = _make_doc(text="Hello Confluence!")
        item = IngestionItem(id="confluence:1", source_ref=doc)
        job = self._make_job()
        self.assertEqual(job.get_raw_content(item), "Hello Confluence!")

    def test_get_raw_content_handles_none_text(self):
        doc = _make_doc(text=None)
        item = IngestionItem(id="confluence:1", source_ref=doc)
        job = self._make_job()
        self.assertEqual(job.get_raw_content(item), "")

    def test_get_item_name_includes_page_id_and_title(self):
        doc = _make_doc(page_id="42", title="My Page")
        item = IngestionItem(id="confluence:42", source_ref=doc)
        job = self._make_job()
        name = job.get_item_name(item)
        self.assertIn("42", name)
        self.assertIn("My_Page", name)

    def test_get_item_name_sanitizes_special_chars(self):
        doc = _make_doc(page_id="99", title="Page / With Slashes & Symbols!")
        item = IngestionItem(id="confluence:99", source_ref=doc)
        job = self._make_job()
        name = job.get_item_name(item)
        self.assertNotIn("/", name)
        self.assertNotIn("&", name)
        self.assertNotIn("!", name)

    def test_get_item_name_truncates_to_255(self):
        doc = _make_doc(page_id="1", title="T" * 300)
        item = IngestionItem(id="confluence:1", source_ref=doc)
        job = self._make_job()
        self.assertLessEqual(len(job.get_item_name(item)), 255)

    def test_get_item_name_no_title_uses_page_id_only(self):
        doc = _make_doc(page_id="77", title="")
        item = IngestionItem(id="confluence:77", source_ref=doc)
        job = self._make_job()
        name = job.get_item_name(item)
        self.assertIn("77", name)

    def test_get_document_metadata_contains_required_fields(self):
        page_url = "https://example.atlassian.net/wiki/spaces/ENG/pages/10"
        doc = _make_doc(page_id="10", title="My Page", url=page_url, space_key="ENG")
        item = IngestionItem(id="confluence:10", source_ref=doc)
        job = self._make_job()
        metadata = job.get_extra_metadata(item=item, _content="", _metadata={})
        self.assertEqual(metadata["title"], "My Page")
        self.assertEqual(metadata["page_id"], "10")
        self.assertEqual(metadata["space_key"], "ENG")
        self.assertEqual(metadata["url"], page_url)

    def test_get_document_metadata_missing_url_falls_back_to_empty_string(self):
        doc = Mock()
        doc.text = "content"
        doc.metadata = {"page_id": "5", "title": "T", "space_key": "ENG"}  # no url key
        item = IngestionItem(id="confluence:5", source_ref=doc)
        job = self._make_job()
        metadata = job.get_extra_metadata(item=item, _content="", _metadata={})
        self.assertEqual(metadata["url"], "")

    def test_reader_constructed_with_oauth2(self):
        oauth2 = {"client_id": "id", "token": {"access_token": "tok"}}
        ConfluenceIngestionJob(
            {"name": "x", "config": {"base_url": "https://x.atlassian.net/wiki", "oauth2": oauth2, "space_key": "ENG"}}
        )
        self.mock_reader_class.assert_called_once_with(
            base_url="https://x.atlassian.net/wiki",
            cloud=True,
            oauth2=oauth2,
            cookies=None,
        )

    def test_reader_constructed_with_cookies(self):
        cookies = {"JSESSIONID": "abc123"}
        ConfluenceIngestionJob(
            {
                "name": "x",
                "config": {"base_url": "https://x.atlassian.net/wiki", "cookies": cookies, "space_key": "ENG"},
            }
        )
        self.mock_reader_class.assert_called_once_with(
            base_url="https://x.atlassian.net/wiki",
            cloud=True,
            cookies=cookies,
        )

    def test_list_items_injects_space_key_from_api(self):
        doc = _make_doc(page_id="55", title="P")
        doc.metadata.pop("space_key", None)
        self.mock_reader.load_data.return_value = [doc]
        self.mock_reader.confluence.get_page_by_id.return_value = {
            "version": {"when": "2024-06-01T00:00:00.000Z"},
            "space": {"key": "PROJ"},
        }
        job = self._make_job()
        items = list(job.list_items())
        self.assertEqual(items[0].source_ref.metadata["space_key"], "PROJ")

    def test_list_items_does_not_overwrite_existing_space_key(self):
        doc = _make_doc(page_id="66", title="P", space_key="ORIG")
        self.mock_reader.load_data.return_value = [doc]
        self.mock_reader.confluence.get_page_by_id.return_value = {
            "version": {"when": "2024-06-01T00:00:00.000Z"},
            "space": {"key": "OTHER"},
        }
        job = self._make_job()
        list(job.list_items())
        self.assertEqual(doc.metadata["space_key"], "ORIG")


if __name__ == "__main__":
    unittest.main()
