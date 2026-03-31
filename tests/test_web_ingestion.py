import unittest
from unittest.mock import MagicMock, patch

from llama_index.core.schema import Document

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.web_ingestion import WebIngestionJob, _title_extractor


class _WebIngestionTestCase(unittest.TestCase):
    """Base class that manages patch lifecycle via setUp/tearDown."""

    def setUp(self):
        self._patches = []

    def tearDown(self):
        for p in self._patches:
            p.stop()

    def _make_job(self, mock_bs_reader=None, mock_sitemap_reader=None, **cfg_overrides):
        cfg = {"name": "web1", "config": cfg_overrides}
        if mock_bs_reader is not None:
            p = patch("tasks.web_ingestion.BeautifulSoupWebReader", return_value=mock_bs_reader)
            self._patches.append(p)
            p.start()
        if mock_sitemap_reader is not None:
            p = patch("tasks.web_ingestion.SitemapReader", return_value=mock_sitemap_reader)
            self._patches.append(p)
            p.start()
        return WebIngestionJob(cfg)


class TestWebIngestionInit(_WebIngestionTestCase):
    def test_init_attributes(self):
        job = self._make_job(
            urls=["https://example.com/page1"],
            include_prefix="/wiki/",
            html_to_text=False,
        )
        self.assertEqual(job.urls, ["https://example.com/page1"])
        self.assertIsNone(job.sitemap_url)
        self.assertEqual(job.include_prefix, "/wiki/")
        self.assertFalse(job.html_to_text)

    def test_html_to_text_defaults_true(self):
        job = self._make_job(urls=["https://example.com"])
        self.assertTrue(job.html_to_text)

    def test_missing_both_raises(self):
        with self.assertRaises(ValueError):
            WebIngestionJob({"name": "web1", "config": {}})

    def test_both_set_raises(self):
        with self.assertRaises(ValueError):
            WebIngestionJob(
                {
                    "name": "web1",
                    "config": {
                        "urls": ["https://example.com"],
                        "sitemap_url": "https://example.com/sitemap.xml",
                    },
                }
            )

    def test_catch_all_extractor_matches_all_hostnames(self):
        job = self._make_job(urls=["https://example.com"])
        self.assertIn("example.com", job.website_extractor)
        self.assertIn("news.ycombinator.com", job.website_extractor)
        self.assertIs(job.website_extractor["example.com"], _title_extractor)
        self.assertIs(job.website_extractor["news.ycombinator.com"], _title_extractor)


class TestWebIngestionListItemsUrls(_WebIngestionTestCase):
    def test_yields_one_item_per_url(self):
        urls = ["https://example.com/a", "https://example.com/b"]
        job = self._make_job(urls=urls)
        items = list(job.list_items())
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].id, "web:https://example.com/a")
        self.assertEqual(items[0].source_ref, "https://example.com/a")
        self.assertEqual(items[1].id, "web:https://example.com/b")


class TestWebIngestionListItemsSitemap(unittest.TestCase):
    def test_yields_items_from_sitemap(self):
        mock_reader = MagicMock()
        mock_reader._load_sitemap.return_value = b"<xml/>"
        mock_reader._parse_sitemap.return_value = [
            "https://example.com/wiki/a",
            "https://example.com/wiki/b",
        ]
        with patch("tasks.web_ingestion.SitemapReader", return_value=mock_reader):
            job = WebIngestionJob(
                {
                    "name": "web1",
                    "config": {
                        "sitemap_url": "https://example.com/sitemap.xml",
                        "include_prefix": "/wiki/",
                    },
                }
            )
            items = list(job.list_items())

        self.assertEqual(len(items), 2)
        mock_reader._parse_sitemap.assert_called_once_with(b"<xml/>", filter_locs="/wiki/")

    def test_no_include_prefix_passes_none(self):
        mock_reader = MagicMock()
        mock_reader._load_sitemap.return_value = b"<xml/>"
        mock_reader._parse_sitemap.return_value = ["https://example.com/page"]
        with patch("tasks.web_ingestion.SitemapReader", return_value=mock_reader):
            job = WebIngestionJob(
                {
                    "name": "web1",
                    "config": {"sitemap_url": "https://example.com/sitemap.xml"},
                }
            )
            list(job.list_items())

        mock_reader._parse_sitemap.assert_called_once_with(b"<xml/>", filter_locs=None)


class TestWebIngestionGetRawContent(unittest.TestCase):
    def test_returns_page_text(self):
        mock_reader = MagicMock()
        mock_reader.load_data.return_value = [Document(text="Hello world", metadata={"title": "Hello"})]
        with patch("tasks.web_ingestion.BeautifulSoupWebReader", return_value=mock_reader):
            job = WebIngestionJob({"name": "web1", "config": {"urls": ["https://example.com"]}})
            item = IngestionItem(id="web:https://example.com", source_ref="https://example.com")
            content = job.get_raw_content(item)

        self.assertEqual(content, "Hello world")
        mock_reader.load_data.assert_called_once_with(urls=["https://example.com"])

    def test_empty_docs_returns_empty_string(self):
        mock_reader = MagicMock()
        mock_reader.load_data.return_value = []
        with patch("tasks.web_ingestion.BeautifulSoupWebReader", return_value=mock_reader):
            job = WebIngestionJob({"name": "web1", "config": {"urls": ["https://example.com"]}})
            item = IngestionItem(id="web:https://example.com", source_ref="https://example.com")
            content = job.get_raw_content(item)

        self.assertEqual(content, "")

    def test_network_error_returns_empty_string(self):
        mock_reader = MagicMock()
        mock_reader.load_data.side_effect = OSError("connection refused")
        with patch("tasks.web_ingestion.BeautifulSoupWebReader", return_value=mock_reader):
            job = WebIngestionJob({"name": "web1", "config": {"urls": ["https://example.com"]}})
            item = IngestionItem(id="web:https://example.com", source_ref="https://example.com")
            content = job.get_raw_content(item)

        self.assertEqual(content, "")

    def test_caches_url_and_title_in_metadata(self):
        mock_reader = MagicMock()
        mock_reader.load_data.return_value = [Document(text="content", metadata={"title": "My Page"})]
        with patch("tasks.web_ingestion.BeautifulSoupWebReader", return_value=mock_reader):
            job = WebIngestionJob({"name": "web1", "config": {"urls": ["https://example.com/p"]}})
            item = IngestionItem(id="web:https://example.com/p", source_ref="https://example.com/p")
            job.get_raw_content(item)

        self.assertEqual(item._metadata_cache["url"], "https://example.com/p")
        self.assertEqual(item._metadata_cache["title"], "My Page")

    def test_title_falls_back_to_url(self):
        mock_reader = MagicMock()
        mock_reader.load_data.return_value = [Document(text="content", metadata={})]
        with patch("tasks.web_ingestion.BeautifulSoupWebReader", return_value=mock_reader):
            job = WebIngestionJob({"name": "web1", "config": {"urls": ["https://example.com/p"]}})
            item = IngestionItem(id="web:https://example.com/p", source_ref="https://example.com/p")
            job.get_raw_content(item)

        self.assertEqual(item._metadata_cache["title"], "https://example.com/p")


class TestWebIngestionGetItemName(_WebIngestionTestCase):
    def test_safe_name_from_url(self):
        job = self._make_job(urls=["https://example.com"])
        item = IngestionItem(id="web:https://example.com/page", source_ref="https://example.com/page")
        name = job.get_item_name(item)
        self.assertNotIn("/", name)
        self.assertNotIn(":", name)
        self.assertLessEqual(len(name), 255)

    def test_special_chars_replaced_with_underscore(self):
        job = self._make_job(urls=["https://example.com"])
        item = IngestionItem(
            id="web:https://example.com/path?q=1&p=2",
            source_ref="https://example.com/path?q=1&p=2",
        )
        name = job.get_item_name(item)
        self.assertRegex(name, r"^[\w\-]+$")


class TestWebIngestionGetDocumentMetadata(_WebIngestionTestCase):
    def test_metadata_contains_url_and_title(self):
        job = self._make_job(urls=["https://example.com"])
        item = IngestionItem(id="web:https://example.com/p", source_ref="https://example.com/p")
        item._metadata_cache["url"] = "https://example.com/p"
        item._metadata_cache["title"] = "My Page"
        metadata = job.get_document_metadata(item, "example_com_p", "abc123", 1, None)
        self.assertEqual(metadata["url"], "https://example.com/p")
        self.assertEqual(metadata["title"], "My Page")
        self.assertEqual(metadata["source"], "web")

    def test_metadata_missing_cache_uses_empty_string(self):
        job = self._make_job(urls=["https://example.com"])
        item = IngestionItem(id="web:https://example.com/p", source_ref="https://example.com/p")
        metadata = job.get_document_metadata(item, "example_com_p", "abc123", 1, None)
        self.assertEqual(metadata["url"], "")
        self.assertEqual(metadata["title"], "")


if __name__ == "__main__":
    unittest.main()
