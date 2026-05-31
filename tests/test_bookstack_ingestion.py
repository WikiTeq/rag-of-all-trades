import unittest
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from tasks.bookstack_ingestion import BookStackIngestionJob


def _make_job(config_overrides=None):
    config = {
        "name": "bookstack-test",
        "config": {
            "base_url": "https://wiki.example.com",
            "token_id": "test-id",
            "token_secret": "test-secret",
        },
    }
    if config_overrides:
        config["config"].update(config_overrides)

    with patch("tasks.base.MetadataTracker"), patch("tasks.base.VectorStoreManager"):
        return BookStackIngestionJob(config)


def _make_item(item_type, data):
    from tasks.helper_classes.ingestion_item import IngestionItem

    return IngestionItem(
        id=f"bookstack:{item_type}:{data['id']}",
        source_ref={"type": item_type, "data": data},
        last_modified=None,
    )


class TestBookStackIngestionInit(unittest.TestCase):
    def test_valid_config(self):
        job = _make_job()
        self.assertEqual(job._client.base_url, "https://wiki.example.com")
        self.assertEqual(set(job.item_types), {"pages"})

    def test_missing_base_url(self):
        with self.assertRaises(ValueError, msg="base_url is required"):
            _make_job({"base_url": ""})

    def test_missing_token_id(self):
        with self.assertRaises(ValueError, msg="token_id is required"):
            _make_job({"token_id": ""})

    def test_missing_token_secret(self):
        with self.assertRaises(ValueError, msg="token_secret is required"):
            _make_job({"token_secret": ""})

    def test_custom_item_types(self):
        job = _make_job({"item_types": ["pages", "books"]})
        self.assertEqual(set(job.item_types), {"pages", "books"})

    def test_invalid_item_type(self):
        with self.assertRaises(ValueError, msg="Invalid item_types"):
            _make_job({"item_types": ["pages", "invalid"]})

    def test_empty_item_types(self):
        with self.assertRaises(ValueError):
            _make_job({"item_types": []})

    def test_base_url_trailing_slash_stripped(self):
        job = _make_job({"base_url": "https://wiki.example.com/"})
        self.assertEqual(job._client.base_url, "https://wiki.example.com")


class TestBookStackListItems(unittest.TestCase):
    def test_list_items_yields_all_types(self):
        job = _make_job({"item_types": ["pages", "books"]})

        page_item = {"id": 1, "name": "Page One", "updated_at": "2024-01-01T00:00:00Z"}
        book_item = {"id": 2, "name": "Book One", "updated_at": "2024-01-02T00:00:00Z"}

        def paginate(endpoint):
            if endpoint == "pages":
                yield page_item
            elif endpoint == "books":
                yield book_item

        job._client = MagicMock()
        job._client.paginate.side_effect = paginate

        items = list(job.list_items())

        self.assertEqual(len(items), 2)
        ids = {i.id for i in items}
        self.assertIn("bookstack:pages:1", ids)
        self.assertIn("bookstack:books:2", ids)

    def test_list_items_pagination(self):
        job = _make_job({"item_types": ["pages"]})
        batch = [{"id": i, "name": f"Page {i}", "updated_at": None} for i in range(101)]

        job._client = MagicMock()
        job._client.paginate.return_value = iter(batch)

        items = list(job.list_items())
        self.assertEqual(len(items), 101)

    def test_list_items_last_modified(self):
        job = _make_job({"item_types": ["books"]})
        book = {"id": 5, "name": "My Book", "updated_at": "2024-06-15T10:00:00Z"}

        job._client = MagicMock()
        job._client.paginate.return_value = iter([book])

        items = list(job.list_items())
        self.assertEqual(items[0].last_modified, datetime(2024, 6, 15, 10, 0, 0, tzinfo=UTC))


class TestBookStackGetRawContent(unittest.TestCase):
    def test_page_prefers_markdown_field(self):
        job = _make_job()
        job._client = MagicMock()
        job._client.base_url = "https://wiki.example.com"
        job._client.get.return_value = {
            "markdown": "# Hello\n\nWorld",
            "raw_html": "<h1>Hello</h1>",
            "html": "<h1>Hello</h1>",
        }

        item = _make_item("pages", {"id": 1, "name": "My Page", "updated_at": None})
        content = job.get_raw_content(item)

        self.assertIn("Hello", content)
        self.assertIn("My Page", content)
        job._client.get.assert_called_once_with("pages/1")

    def test_page_falls_back_to_raw_html(self):
        job = _make_job()
        job._client = MagicMock()
        job._client.base_url = "https://wiki.example.com"
        job._client.get.return_value = {
            "markdown": "",
            "raw_html": "<h1>Hello</h1><p>World</p>",
            "html": "<h1>escaped</h1>",
        }

        item = _make_item("pages", {"id": 2, "name": "My Page", "updated_at": None})
        content = job.get_raw_content(item)

        self.assertIn("Hello", content)

    def test_page_url_uses_link_format(self):
        job = _make_job()
        job._client = MagicMock()
        job._client.base_url = "https://wiki.example.com"
        job._client.get.return_value = {"markdown": "content", "raw_html": "", "html": ""}

        item = _make_item("pages", {"id": 5, "name": "A Page", "updated_at": None})
        job.get_raw_content(item)
        self.assertEqual(item._metadata_cache["url"], "https://wiki.example.com/link/5")

    def test_book_content_uses_description(self):
        job = _make_job()
        item = _make_item(
            "books", {"id": 2, "name": "My Book", "description": "A great book", "slug": "my-book", "updated_at": None}
        )

        content = job.get_raw_content(item)

        self.assertIn("My Book", content)
        self.assertIn("A great book", content)

    def test_empty_description_returns_name_only(self):
        job = _make_job()
        item = _make_item(
            "books", {"id": 3, "name": "Empty Book", "description": "", "slug": "empty-book", "updated_at": None}
        )

        content = job.get_raw_content(item)

        self.assertIn("Empty Book", content)

    def test_non_page_url_uses_slug(self):
        job = _make_job()
        item = _make_item(
            "books", {"id": 4, "name": "Book", "description": "", "slug": "my-book-slug", "updated_at": None}
        )
        job.get_raw_content(item)
        self.assertEqual(item._metadata_cache["url"], "https://wiki.example.com/books/my-book-slug")


class TestBookStackGetItemChecksum(unittest.TestCase):
    def test_checksum_uses_id_and_updated_at(self):
        job = _make_job()
        item = _make_item("pages", {"id": 7, "name": "Page", "updated_at": "2024-03-01T12:00:00Z"})
        self.assertEqual(job.get_item_checksum(item), "7:2024-03-01T12:00:00Z")

    def test_checksum_returns_none_when_no_updated_at(self):
        job = _make_job()
        item = _make_item("pages", {"id": 7, "name": "Page", "updated_at": None})
        self.assertIsNone(job.get_item_checksum(item))


class TestBookStackGetItemName(unittest.TestCase):
    def test_name_format(self):
        job = _make_job()
        item = _make_item("pages", {"id": 42, "name": "My Page Title"})
        name = job.get_item_name(item)
        self.assertTrue(name.startswith("bookstack-pages-42-"))
        self.assertIn("My_Page_Title", name)

    def test_name_max_length(self):
        job = _make_job()
        item = _make_item("pages", {"id": 1, "name": "A" * 300})
        name = job.get_item_name(item)
        self.assertLessEqual(len(name), 255)


class TestBookStackGetExtraMetadata(unittest.TestCase):
    def _make_item_with_cache(self, item_type, data, detail=None):
        item = _make_item(item_type, data)
        item._metadata_cache["url"] = f"https://wiki.example.com/{item_type}/{data['id']}"
        item._metadata_cache["title"] = data.get("name", "")
        if detail is not None:
            item._metadata_cache["detail"] = detail
        return item

    def test_extra_metadata_base_fields(self):
        job = _make_job()
        job._book_shelf_map = {}
        item = self._make_item_with_cache("books", {"id": 1, "name": "Test Book", "updated_at": "2024-01-01T00:00:00Z"})
        meta = job.get_extra_metadata(item, "content", {})

        self.assertEqual(meta["item_type"], "books")
        self.assertEqual(meta["title"], "Test Book")
        self.assertEqual(meta["updated_at"], "2024-01-01T00:00:00Z")

    def test_extra_metadata_page_fields(self):
        job = _make_job()
        detail = {
            "owned_by": {"name": "Alice"},
            "updated_by": {"name": "Bob"},
            "draft": False,
            "tags": [{"name": "tag1"}, {"name": "tag2"}],
        }
        item = self._make_item_with_cache(
            "pages",
            {"id": 1, "name": "Test Page", "updated_at": "2024-01-01T00:00:00Z", "book_id": 10, "chapter_id": 5},
            detail=detail,
        )
        meta = job.get_extra_metadata(item, "content", {})

        self.assertEqual(meta["owner"], "Alice")
        self.assertEqual(meta["editor"], "Bob")
        self.assertEqual(meta["draft"], "False")
        self.assertEqual(meta["tags"], "tag1,tag2")
        self.assertEqual(meta["book_id"], "10")
        self.assertEqual(meta["chapter_id"], "5")

    def test_extra_metadata_shelf_tags(self):
        job = _make_job()
        item = self._make_item_with_cache(
            "shelves",
            {"id": 2, "name": "Shelf", "updated_at": None, "tags": [{"name": "docs"}]},
        )
        meta = job.get_extra_metadata(item, "content", {})
        self.assertEqual(meta["tags"], "docs")

    def test_get_book_shelf_map_builds_from_shelves(self):
        job = _make_job()
        job._client.paginate = MagicMock(
            return_value=iter([{"id": 99, "books": [{"id": 7}, {"id": 8}]}, {"id": 100, "books": [{"id": 9}]}])
        )
        result = job._get_book_shelf_map()
        self.assertEqual(result, {7: 99, 8: 99, 9: 100})
        job._client.paginate.assert_called_once_with("shelves")

    def test_get_book_shelf_map_cached(self):
        job = _make_job()
        job._book_shelf_map = {1: 2}
        result = job._get_book_shelf_map()
        self.assertEqual(result, {1: 2})

    def test_extra_metadata_book_resolves_shelf_id(self):
        job = _make_job()
        job._book_shelf_map = {7: 99, 8: 99}
        item = self._make_item_with_cache("books", {"id": 7, "name": "My Book", "updated_at": None})
        meta = job.get_extra_metadata(item, "content", {})
        self.assertEqual(meta["shelf_id"], "99")

    def test_extra_metadata_book_no_shelf(self):
        job = _make_job()
        job._book_shelf_map = {}
        item = self._make_item_with_cache("books", {"id": 7, "name": "Orphan Book", "updated_at": None})
        meta = job.get_extra_metadata(item, "content", {})
        self.assertEqual(meta["shelf_id"], "")

    def test_extra_metadata_chapter_resolves_shelf_id(self):
        job = _make_job()
        job._book_shelf_map = {7: 99}
        item = self._make_item_with_cache("chapters", {"id": 3, "name": "Ch 1", "book_id": 7, "updated_at": None})
        meta = job.get_extra_metadata(item, "content", {})
        self.assertEqual(meta["shelf_id"], "99")
        self.assertEqual(meta["book_id"], "7")

    def test_extra_metadata_page_resolves_shelf_id(self):
        job = _make_job()
        job._book_shelf_map = {7: 99}
        item = self._make_item_with_cache(
            "pages",
            {"id": 5, "name": "Page", "book_id": 7, "chapter_id": 3, "updated_at": None},
        )
        meta = job.get_extra_metadata(item, "content", {})
        self.assertEqual(meta["shelf_id"], "99")

    def test_no_reserved_keys_overwritten(self):
        from tasks.base import IngestionJob

        job = _make_job()
        job._book_shelf_map = {}
        item = self._make_item_with_cache("books", {"id": 2, "name": "Book", "updated_at": None})
        meta = job.get_extra_metadata(item, "content", {})
        for key in IngestionJob.RESERVED_METADATA_KEYS:
            self.assertNotIn(key, meta)


if __name__ == "__main__":
    unittest.main()
