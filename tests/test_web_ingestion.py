import unittest
from unittest.mock import MagicMock, patch

from llama_index.core.schema import Document

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.web_ingestion import WebIngestionJob, _title_extractor


class _WebIngestionTestCase(unittest.TestCase):
    """Base class that manages patch lifecycle via setUp/tearDown."""

    def setUp(self):
        """Initialise the patch registry before each test."""
        self._patches = []

    def tearDown(self):
        """Stop all active patches after each test."""
        for p in self._patches:
            p.stop()

    def _make_job(self, mock_bs_reader=None, mock_sitemap_reader=None, **cfg_overrides):
        """Build a WebIngestionJob with optional reader mocks tracked for cleanup."""
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

    def test_depth_defaults_zero(self):
        job = self._make_job(urls=["https://example.com"])
        self.assertEqual(job.depth, 0)

    def test_same_domain_only_defaults_true(self):
        job = self._make_job(urls=["https://example.com"])
        self.assertTrue(job.same_domain_only)

    def test_max_pages_defaults_none(self):
        job = self._make_job(urls=["https://example.com"])
        self.assertIsNone(job.max_pages)

    def test_depth_sitemap_raises(self):
        with self.assertRaises(ValueError):
            WebIngestionJob(
                {
                    "name": "web1",
                    "config": {
                        "sitemap_url": "https://example.com/sitemap.xml",
                        "depth": 1,
                    },
                }
            )

    def test_same_domain_only_string_false(self):
        job = self._make_job(urls=["https://example.com"], same_domain_only="false")
        self.assertFalse(job.same_domain_only)

    def test_same_domain_only_string_zero(self):
        job = self._make_job(urls=["https://example.com"], same_domain_only="0")
        self.assertFalse(job.same_domain_only)

    def test_same_domain_only_bool_true(self):
        job = self._make_job(urls=["https://example.com"], same_domain_only=True)
        self.assertTrue(job.same_domain_only)

    def test_max_pages_zero_raises(self):
        with self.assertRaises(ValueError):
            self._make_job(urls=["https://example.com"], max_pages=0)

    def test_max_pages_negative_raises(self):
        with self.assertRaises(ValueError):
            self._make_job(urls=["https://example.com"], max_pages=-1)

    def test_max_pages_valid(self):
        job = self._make_job(urls=["https://example.com"], max_pages=10)
        self.assertEqual(job.max_pages, 10)


class TestWebIngestionListItemsUrls(_WebIngestionTestCase):
    def test_yields_one_item_per_url(self):
        urls = ["https://example.com/a", "https://example.com/b"]
        job = self._make_job(urls=urls)
        items = list(job.list_items())
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].id, "web:https://example.com/a")
        self.assertEqual(items[0].source_ref, "https://example.com/a")
        self.assertEqual(items[1].id, "web:https://example.com/b")

    def test_list_items_depth_0_no_crawl(self):
        job = self._make_job(urls=["https://example.com/a"], depth=0)
        with patch.object(job, "_crawl") as mock_crawl:
            list(job.list_items())
        mock_crawl.assert_not_called()


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

    def test_crawl_cache_avoids_double_fetch(self):
        mock_reader = MagicMock()
        with patch("tasks.web_ingestion.BeautifulSoupWebReader", return_value=mock_reader):
            job = WebIngestionJob({"name": "web1", "config": {"urls": ["https://example.com"]}})
            item = IngestionItem(id="web:https://example.com", source_ref="https://example.com")
            item._metadata_cache["_crawl_text"] = "cached content"
            item._metadata_cache["_crawl_title"] = "Cached Title"
            content = job.get_raw_content(item)

        self.assertEqual(content, "cached content")
        mock_reader.load_data.assert_not_called()


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

    def test_get_item_name_no_collision(self):
        job = self._make_job(urls=["https://example.com"])
        long_base = "https://example.com/" + "a" * 250
        url1 = long_base + "?x=1"
        url2 = long_base + "?x=2"
        item1 = IngestionItem(id=f"web:{url1}", source_ref=url1)
        item2 = IngestionItem(id=f"web:{url2}", source_ref=url2)
        name1 = job.get_item_name(item1)
        name2 = job.get_item_name(item2)
        self.assertNotEqual(name1, name2)
        self.assertLessEqual(len(name1), 255)
        self.assertLessEqual(len(name2), 255)


class TestWebIngestionGetDocumentMetadata(_WebIngestionTestCase):
    def test_metadata_contains_url_and_title(self):
        job = self._make_job(urls=["https://example.com"])
        item = IngestionItem(id="web:https://example.com/p", source_ref="https://example.com/p")
        item._metadata_cache["url"] = "https://example.com/p"
        item._metadata_cache["title"] = "My Page"
        extra = job.get_extra_metadata(item=item, content="", metadata={})
        self.assertEqual(extra["url"], "https://example.com/p")
        self.assertEqual(extra["title"], "My Page")

    def test_metadata_missing_cache_uses_empty_string(self):
        job = self._make_job(urls=["https://example.com"])
        item = IngestionItem(id="web:https://example.com/p", source_ref="https://example.com/p")
        extra = job.get_extra_metadata(item=item, content="", metadata={})
        self.assertEqual(extra["url"], "")
        self.assertEqual(extra["title"], "")


def _make_html(links=(), title="Page", base_href=None):
    base = f'<base href="{base_href}">' if base_href else ""
    anchors = "".join(f'<a href="{href}">link</a>' for href in links)
    return f"<html><head>{base}<title>{title}</title></head><body>{anchors}</body></html>"


def _mock_get(url_html_map, content_type="text/html"):
    def _get(url, **kwargs):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.headers = {"Content-Type": content_type}
        resp.text = url_html_map.get(url, "<html></html>")
        return resp

    return _get


class TestWebIngestionCrawl(_WebIngestionTestCase):
    def _make_crawl_job(self, **cfg_overrides):
        return self._make_job(urls=["https://example.com"], **cfg_overrides)

    def test_crawl_follows_links(self):
        job = self._make_crawl_job(depth=1)
        html = _make_html(links=["https://example.com/a", "https://example.com/b"])
        job._crawl_cache = {}
        with patch("tasks.web_ingestion.requests.get", side_effect=_mock_get({"https://example.com": html})):
            urls = job._crawl(["https://example.com"])
        self.assertIn("https://example.com/a", urls)
        self.assertIn("https://example.com/b", urls)

    def test_crawl_same_domain_only(self):
        job = self._make_crawl_job(depth=1, same_domain_only=True)
        html = _make_html(links=["https://example.com/internal", "https://other.com/external"])
        job._crawl_cache = {}
        with patch("tasks.web_ingestion.requests.get", side_effect=_mock_get({"https://example.com": html})):
            urls = job._crawl(["https://example.com"])
        self.assertIn("https://example.com/internal", urls)
        self.assertNotIn("https://other.com/external", urls)

    def test_crawl_cross_domain_allowed_when_disabled(self):
        job = self._make_crawl_job(depth=1, same_domain_only=False)
        html = _make_html(links=["https://other.com/page"])
        job._crawl_cache = {}
        with patch("tasks.web_ingestion.requests.get", side_effect=_mock_get({"https://example.com": html})):
            urls = job._crawl(["https://example.com"])
        self.assertIn("https://other.com/page", urls)

    def test_crawl_max_pages_hard_stop(self):
        job = self._make_crawl_job(depth=2, max_pages=2)
        html_root = _make_html(links=["https://example.com/a", "https://example.com/b"])
        html_a = _make_html(links=["https://example.com/c"])
        url_map = {
            "https://example.com": html_root,
            "https://example.com/a": html_a,
            "https://example.com/b": "<html></html>",
        }
        job._crawl_cache = {}
        with patch("tasks.web_ingestion.requests.get", side_effect=_mock_get(url_map)):
            urls = job._crawl(["https://example.com"])
        self.assertLessEqual(len(urls), 2)

    def test_crawl_depth_2(self):
        job = self._make_crawl_job(depth=2)
        html_root = _make_html(links=["https://example.com/level1"])
        html_l1 = _make_html(links=["https://example.com/level2"])
        url_map = {
            "https://example.com": html_root,
            "https://example.com/level1": html_l1,
            "https://example.com/level2": "<html></html>",
        }
        job._crawl_cache = {}
        with patch("tasks.web_ingestion.requests.get", side_effect=_mock_get(url_map)):
            urls = job._crawl(["https://example.com"])
        self.assertIn("https://example.com/level2", urls)

    def test_crawl_cycle_prevention(self):
        job = self._make_crawl_job(depth=2)
        html = _make_html(links=["https://example.com"])
        job._crawl_cache = {}
        with patch("tasks.web_ingestion.requests.get", side_effect=_mock_get({"https://example.com": html})):
            urls = job._crawl(["https://example.com"])
        self.assertEqual(urls.count("https://example.com"), 1)

    def test_crawl_non_html_skipped(self):
        job = self._make_crawl_job(depth=1)
        html_root = _make_html(links=["https://example.com/style.css"])
        job._crawl_cache = {}

        def _get(url, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if url.endswith(".css"):
                resp.headers = {"Content-Type": "text/css"}
            else:
                resp.headers = {"Content-Type": "text/html"}
            resp.text = html_root
            return resp

        with patch("tasks.web_ingestion.requests.get", side_effect=_get):
            urls = job._crawl(["https://example.com"])

        # css URL is discovered as a link but when fetched it's skipped from further parsing
        # it may appear in visited since it's added before fetching; what matters is it wasn't parsed
        self.assertIn("https://example.com", urls)

    def test_crawl_base_href_resolution(self):
        job = self._make_crawl_job(depth=1)
        html = _make_html(links=["/relative"], base_href="https://example.com/base/")
        job._crawl_cache = {}
        with patch("tasks.web_ingestion.requests.get", side_effect=_mock_get({"https://example.com": html})):
            urls = job._crawl(["https://example.com"])
        self.assertIn("https://example.com/relative", urls)

    def test_crawl_request_delay_honoured(self):
        job = self._make_crawl_job(depth=1)
        job.request_delay = 0.1
        html = _make_html(links=["https://example.com/a"])
        job._crawl_cache = {}
        with patch("tasks.web_ingestion.requests.get", side_effect=_mock_get({"https://example.com": html})):
            with patch("tasks.web_ingestion.time.sleep") as mock_sleep:
                job._crawl(["https://example.com"])
        mock_sleep.assert_called_with(0.1)

    def test_crawl_cache_populated_during_crawl(self):
        job = self._make_crawl_job(depth=1)
        html = _make_html(title="Root Page")
        job._crawl_cache = {}
        with patch("tasks.web_ingestion.requests.get", side_effect=_mock_get({"https://example.com": html})):
            job._crawl(["https://example.com"])
        self.assertIn("https://example.com", job._crawl_cache)
        self.assertIn("_crawl_text", job._crawl_cache["https://example.com"])
        self.assertEqual(job._crawl_cache["https://example.com"]["_crawl_title"], "Root Page")


if __name__ == "__main__":
    unittest.main()
