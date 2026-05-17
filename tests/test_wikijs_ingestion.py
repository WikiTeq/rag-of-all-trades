import unittest
from unittest.mock import Mock, patch

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.wikijs_ingestion import WikiJsIngestionJob
from utils.graphql import GraphQLError


def _make_config(**kwargs):
    cfg = {
        "base_url": "https://wiki.example.com",
        "api_token": "test-token",
    }
    cfg.update(kwargs)
    return {"name": "wikijs-test", "config": cfg}


def _make_page(
    page_id=1, path="/test/page", title="Test Page", updated_at="2024-06-01T12:00:00.000Z", is_published=True
):
    return {
        "id": page_id,
        "path": path,
        "title": title,
        "updatedAt": updated_at,
        "isPublished": is_published,
    }


def _make_page_detail(page_id=1, path="/test/page", title="Test Page", content="Some content", content_type="markdown"):
    return {
        "id": page_id,
        "path": path,
        "title": title,
        "updatedAt": "2024-06-01T12:00:00.000Z",
        "content": content,
        "contentType": content_type,
    }


class TestWikiJsIngestionJob(unittest.TestCase):
    def setUp(self):
        self.client_patcher = patch("tasks.wikijs_ingestion.WikiJsClient")
        self.mock_client_class = self.client_patcher.start()
        self.mock_client = Mock()
        self.mock_client_class.return_value = self.mock_client
        self.mock_client.base_url = "https://wiki.example.com"

    def tearDown(self):
        self.client_patcher.stop()

    def _make_job(self, **kwargs):
        return WikiJsIngestionJob(_make_config(**kwargs))

    def test_source_type(self):
        job = self._make_job()
        self.assertEqual(job.source_type, "wikijs")

    def test_missing_base_url_raises(self):
        with self.assertRaises(ValueError):
            WikiJsIngestionJob({"name": "x", "config": {"api_token": "tok"}})

    def test_missing_api_token_raises(self):
        with self.assertRaises(ValueError):
            WikiJsIngestionJob({"name": "x", "config": {"base_url": "https://wiki.example.com"}})

    def test_optional_fields_default(self):
        job = self._make_job()
        self.assertEqual(job.paths, [])
        self.assertEqual(job.tags, [])
        self.assertIsNone(job.locale)
        self.assertFalse(job.include_unpublished)

    def test_optional_fields_parsed(self):
        job = self._make_job(paths="/eng,/product", tags="public,internal", locale="en", include_unpublished="true")
        self.assertEqual(job.paths, ["/eng", "/product"])
        self.assertEqual(job.tags, ["public", "internal"])
        self.assertEqual(job.locale, "en")
        self.assertTrue(job.include_unpublished)

    def test_paths_normalized_with_leading_slash(self):
        job = self._make_job(paths="engineering,/product/")
        self.assertEqual(job.paths, ["/engineering", "/product"])

    def test_list_items_yields_all_published_pages(self):
        self.mock_client.list_pages.return_value = [
            _make_page(1, "/a", "A"),
            _make_page(2, "/b", "B"),
        ]
        job = self._make_job()
        items = list(job.list_items())
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].id, "wikijs:1")
        self.assertEqual(items[1].id, "wikijs:2")
        self.assertIsInstance(items[0], IngestionItem)

    def test_list_items_skips_page_with_missing_id(self):
        self.mock_client.list_pages.return_value = [
            {"path": "/broken", "title": "No ID", "updatedAt": None, "isPublished": True},
            _make_page(2, "/ok", "OK"),
        ]
        job = self._make_job()
        items = list(job.list_items())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "wikijs:2")

    def test_list_items_returns_empty_on_graphql_error(self):
        self.mock_client.list_pages.side_effect = GraphQLError("query failed")
        job = self._make_job()
        items = list(job.list_items())
        self.assertEqual(items, [])

    def test_list_items_reraises_unexpected_exception(self):
        self.mock_client.list_pages.side_effect = RuntimeError("network error")
        job = self._make_job()
        with self.assertRaises(RuntimeError):
            list(job.list_items())

    def test_list_items_filters_unpublished_by_default(self):
        self.mock_client.list_pages.return_value = [
            _make_page(1, "/a", "A", is_published=True),
            _make_page(2, "/b", "B", is_published=False),
        ]
        job = self._make_job()
        items = list(job.list_items())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "wikijs:1")

    def test_list_items_includes_unpublished_when_configured(self):
        self.mock_client.list_pages.return_value = [
            _make_page(1, "/a", "A", is_published=True),
            _make_page(2, "/b", "B", is_published=False),
        ]
        job = self._make_job(include_unpublished="true")
        items = list(job.list_items())
        self.assertEqual(len(items), 2)

    def test_list_items_filters_by_path_prefix(self):
        self.mock_client.list_pages.return_value = [
            _make_page(1, "engineering/setup"),
            _make_page(2, "product/roadmap"),
            _make_page(3, "hr/policy"),
        ]
        job = self._make_job(paths="/engineering,/product")
        items = list(job.list_items())
        self.assertEqual(len(items), 2)
        ids = {i.id for i in items}
        self.assertIn("wikijs:1", ids)
        self.assertIn("wikijs:2", ids)

    def test_list_items_path_filter_does_not_match_prefix_of_longer_path(self):
        self.mock_client.list_pages.return_value = [
            _make_page(1, "docs/guide"),
            _make_page(2, "docs-old/guide"),
        ]
        job = self._make_job(paths="/docs")
        items = list(job.list_items())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "wikijs:1")

    def test_list_items_passes_tags_and_locale_to_client(self):
        self.mock_client.list_pages.return_value = []
        job = self._make_job(tags="public", locale="en")
        list(job.list_items())
        self.mock_client.list_pages.assert_called_once_with(tags=["public"], locale="en")

    def test_list_items_last_modified_parsed(self):
        self.mock_client.list_pages.return_value = [_make_page(1, updated_at="2024-06-15T10:30:00.000Z")]
        job = self._make_job()
        items = list(job.list_items())
        self.assertIsNotNone(items[0].last_modified)
        self.assertEqual(items[0].last_modified.year, 2024)
        self.assertEqual(items[0].last_modified.month, 6)

    def test_get_item_checksum_uses_updated_at(self):
        page = _make_page(1, updated_at="2024-06-01T12:00:00.000Z")
        item = IngestionItem(id="wikijs:1", source_ref=page)
        job = self._make_job()
        checksum = job.get_item_checksum(item)
        self.assertIsNotNone(checksum)
        self.assertIn("1", checksum)
        self.assertIn("2024", checksum)

    def test_get_item_checksum_none_when_no_updated_at(self):
        page = _make_page(1, updated_at="")
        item = IngestionItem(id="wikijs:1", source_ref=page)
        job = self._make_job()
        self.assertIsNone(job.get_item_checksum(item))

    def test_get_raw_content_markdown(self):
        page = _make_page(1, "/test", "My Page")
        item = IngestionItem(id="wikijs:1", source_ref=page)
        self.mock_client.get_page.return_value = _make_page_detail(
            1, title="My Page", content="Hello world", content_type="markdown"
        )

        job = self._make_job()
        content = job.get_raw_content(item)

        self.assertIn("# My Page", content)
        self.assertIn("Hello world", content)

    def test_get_raw_content_html_converted_to_markdown(self):
        page = _make_page(1, "/test", "HTML Page")
        item = IngestionItem(id="wikijs:1", source_ref=page)
        self.mock_client.get_page.return_value = _make_page_detail(
            1, title="HTML Page", content="<h2>Section</h2><p>Body text</p>", content_type="html"
        )

        job = self._make_job()
        content = job.get_raw_content(item)

        self.assertIn("# HTML Page", content)
        self.assertIn("Section", content)
        self.assertIn("Body text", content)
        self.assertNotIn("<h2>", content)
        self.assertNotIn("<p>", content)

    def test_get_raw_content_html_content_type_case_insensitive(self):
        page = _make_page(1, "/test", "HTML Page")
        item = IngestionItem(id="wikijs:1", source_ref=page)
        self.mock_client.get_page.return_value = _make_page_detail(
            1, title="HTML Page", content="<p>Body</p>", content_type="HTML"
        )
        job = self._make_job()
        content = job.get_raw_content(item)
        self.assertNotIn("<p>", content)

    def test_get_raw_content_empty_warns_and_returns_empty(self):
        page = _make_page(1, "/test", "Empty Page")
        item = IngestionItem(id="wikijs:1", source_ref=page)
        self.mock_client.get_page.return_value = _make_page_detail(1, title="Empty Page", content="")

        job = self._make_job()
        with self.assertLogs("tasks.wikijs_ingestion", level="WARNING") as cm:
            content = job.get_raw_content(item)

        self.assertIn("read:source", cm.output[0])
        self.assertEqual(content, "")

    def test_get_raw_content_caches_url_and_title(self):
        page = _make_page(1, "/eng/setup", "Setup Guide")
        item = IngestionItem(id="wikijs:1", source_ref=page)
        self.mock_client.get_page.return_value = _make_page_detail(1, path="/eng/setup", title="Setup Guide")

        job = self._make_job()
        job.get_raw_content(item)

        self.assertEqual(item._metadata_cache["title"], "Setup Guide")
        self.assertIn("eng/setup", item._metadata_cache["url"])

    def test_get_item_name_uses_only_stable_id(self):
        page = _make_page(42, "/test", "My Page")
        item = IngestionItem(id="wikijs:42", source_ref=page)
        job = self._make_job()
        name = job.get_item_name(item)
        self.assertEqual(name, "wikijs-42")
        self.assertLessEqual(len(name), 255)

    def test_get_extra_metadata_fields(self):
        page = _make_page(7, "/eng/guide", "Guide", "2024-06-01T12:00:00.000Z")
        item = IngestionItem(id="wikijs:7", source_ref=page)
        object.__setattr__(item, "_metadata_cache", {"title": "Guide", "url": "https://wiki.example.com/eng/guide"})

        job = self._make_job()
        extra = job.get_extra_metadata(item=item, content="", metadata={})

        self.assertEqual(extra["page_id"], "7")
        self.assertEqual(extra["path"], "/eng/guide")
        self.assertEqual(extra["title"], "Guide")
        self.assertEqual(extra["url"], "https://wiki.example.com/eng/guide")
        self.assertIn("2024", extra["updated_at"])


if __name__ == "__main__":
    unittest.main()
