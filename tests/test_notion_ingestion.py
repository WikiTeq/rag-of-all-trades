import unittest
from unittest.mock import MagicMock, patch

import httpx
from notion_client.errors import APIResponseError

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.notion_ingestion import NotionIngestionJob


def _api_error(status=404, message="Not found", code="not_found"):
    return APIResponseError(
        code=code,
        status=status,
        message=message,
        headers=httpx.Headers({}),
        raw_body_text=message,
    )


def _make_config(
    integration_token="ntn_test",
    page_ids="",
    database_ids="",
    request_delay=0,
):
    return {
        "name": "test_notion",
        "config": {
            "integration_token": integration_token,
            "page_ids": page_ids,
            "database_ids": database_ids,
            "request_delay": request_delay,
        },
    }


def _make_page(page_id="page-1", title="Test Page", in_trash=False):
    return {
        "object": "page",
        "id": page_id,
        "in_trash": in_trash,
        "archived": False,
        "last_edited_time": "2024-01-01T00:00:00.000Z",
        "created_time": "2024-01-01T00:00:00.000Z",
        "url": f"https://notion.so/{page_id}",
        "public_url": None,
        "parent": {"type": "workspace", "workspace": True},
        "created_by": {"id": "user-1"},
        "last_edited_by": {"id": "user-1"},
        "properties": {
            "title": {
                "type": "title",
                "title": [{"plain_text": title, "text": {"content": title}}],
            }
        },
    }


def _make_job(mock_client, **kwargs):
    with patch("tasks.notion_ingestion.Client", return_value=mock_client):
        return NotionIngestionJob(_make_config(**kwargs))


class TestNotionIngestionInit(unittest.TestCase):
    def test_source_type(self):
        mock_client = MagicMock()
        job = _make_job(mock_client)
        self.assertEqual(job.source_type, "notion")

    def test_missing_integration_token_raises(self):
        with self.assertRaises(ValueError):
            with patch("tasks.notion_ingestion.Client"):
                NotionIngestionJob({"name": "x", "config": {}})

    def test_negative_request_delay_raises(self):
        with self.assertRaises(ValueError):
            _make_job(MagicMock(), request_delay=-1)

    def test_client_initialized_with_token(self):
        with patch("tasks.notion_ingestion.Client") as mock_cls:
            NotionIngestionJob(_make_config(integration_token="ntn_abc"))
            mock_cls.assert_called_once_with(auth="ntn_abc")


class TestNotionParseIds(unittest.TestCase):
    def test_parse_ids(self):
        self.assertEqual(NotionIngestionJob._parse_ids("a, b, c"), ["a", "b", "c"])
        self.assertEqual(NotionIngestionJob._parse_ids(["a", "b"]), ["a", "b"])
        self.assertEqual(NotionIngestionJob._parse_ids(""), [])
        self.assertEqual(NotionIngestionJob._parse_ids(None), [])


class TestNotionListItemsSelective(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.mock_client.users.retrieve.return_value = {"name": "Alice"}

    def test_yields_configured_page_ids(self):
        self.mock_client.pages.retrieve.side_effect = lambda page_id: _make_page(page_id)
        job = _make_job(self.mock_client, page_ids="page-1,page-2")
        items = list(job.list_items())

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].id, "notion:page-1")
        self.assertEqual(items[1].id, "notion:page-2")

    def test_skips_failed_page_fetch(self):
        self.mock_client.pages.retrieve.side_effect = _api_error(404, "Not found", "object_not_found")
        job = _make_job(self.mock_client, page_ids="bad-page")
        items = list(job.list_items())
        self.assertEqual(items, [])

    def test_resolves_database_ids(self):
        self.mock_client.data_sources.query.return_value = {
            "results": [{"object": "page", "id": "db-page-1"}],
            "has_more": False,
        }
        self.mock_client.pages.retrieve.side_effect = lambda page_id: _make_page(page_id)
        job = _make_job(self.mock_client, database_ids="db-1")
        items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "notion:db-page-1")

    def test_database_query_error_continues(self):
        self.mock_client.data_sources.query.side_effect = _api_error(404, "Not found", "object_not_found")
        job = _make_job(self.mock_client, database_ids="db-bad")
        items = list(job.list_items())
        self.assertEqual(items, [])

    def test_database_query_paginates(self):
        self.mock_client.data_sources.query.side_effect = [
            {"results": [{"object": "page", "id": "db-page-1"}], "has_more": True, "next_cursor": "cursor-1"},
            {"results": [{"object": "page", "id": "db-page-2"}], "has_more": False},
        ]
        self.mock_client.pages.retrieve.side_effect = lambda page_id: _make_page(page_id)
        job = _make_job(self.mock_client, database_ids="db-1")
        items = list(job.list_items())
        self.assertEqual(len(items), 2)

    def test_skips_trashed_pages(self):
        self.mock_client.pages.retrieve.return_value = _make_page(in_trash=True)
        job = _make_job(self.mock_client, page_ids="page-1")
        items = list(job.list_items())
        self.assertEqual(items, [])


class TestNotionListItemsLoadAll(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.mock_client.users.retrieve.return_value = {"name": "Alice"}

    def test_uses_search_api(self):
        self.mock_client.search.return_value = {
            "results": [_make_page("page-1")],
            "has_more": False,
        }
        job = _make_job(self.mock_client)
        items = list(job.list_items())

        self.mock_client.search.assert_called_once()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "notion:page-1")

    def test_paginates_search_results(self):
        self.mock_client.search.side_effect = [
            {"results": [_make_page("page-1")], "has_more": True, "next_cursor": "cursor-1"},
            {"results": [_make_page("page-2")], "has_more": False},
        ]
        job = _make_job(self.mock_client)
        items = list(job.list_items())
        self.assertEqual(len(items), 2)

    def test_search_error_yields_nothing(self):
        self.mock_client.search.side_effect = _api_error(500, "Server error", "internal_server_error")
        job = _make_job(self.mock_client)
        items = list(job.list_items())
        self.assertEqual(items, [])


class TestNotionGetRawContent(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()

    def test_returns_page_text(self):
        self.mock_client.blocks.children.list.return_value = {
            "results": [
                {
                    "type": "paragraph",
                    "id": "block-1",
                    "has_children": False,
                    "paragraph": {"rich_text": [{"text": {"content": "Hello world"}}]},
                }
            ],
            "next_cursor": None,
        }
        job = _make_job(self.mock_client)
        item = IngestionItem(id="notion:page-1", source_ref="page-1")
        content = job.get_raw_content(item)
        self.assertIn("Hello world", content)

    def test_returns_empty_on_error(self):
        self.mock_client.blocks.children.list.side_effect = Exception("API error")
        job = _make_job(self.mock_client)
        item = IngestionItem(id="notion:page-1", source_ref="page-1")
        content = job.get_raw_content(item)
        self.assertEqual(content, "")

    def test_applies_request_delay(self):
        self.mock_client.blocks.children.list.return_value = {"results": [], "next_cursor": None}
        job = _make_job(self.mock_client, request_delay=0.01)
        item = IngestionItem(id="notion:page-1", source_ref="page-1")
        with patch("tasks.notion_ingestion.time.sleep") as mock_sleep:
            job.get_raw_content(item)
            mock_sleep.assert_called_once_with(0.01)

    def test_paginates_blocks(self):
        self.mock_client.blocks.children.list.side_effect = [
            {
                "results": [
                    {
                        "type": "paragraph",
                        "id": "block-1",
                        "has_children": False,
                        "paragraph": {"rich_text": [{"text": {"content": "Page 1"}}]},
                    }
                ],
                "next_cursor": "cursor-1",
            },
            {
                "results": [
                    {
                        "type": "paragraph",
                        "id": "block-2",
                        "has_children": False,
                        "paragraph": {"rich_text": [{"text": {"content": "Page 2"}}]},
                    }
                ],
                "next_cursor": None,
            },
        ]
        job = _make_job(self.mock_client)
        item = IngestionItem(id="notion:page-1", source_ref="page-1")
        content = job.get_raw_content(item)
        self.assertIn("Page 1", content)
        self.assertIn("Page 2", content)

    def test_recurses_into_children(self):
        self.mock_client.blocks.children.list.side_effect = [
            {
                "results": [
                    {
                        "type": "paragraph",
                        "id": "child-block",
                        "has_children": True,
                        "paragraph": {"rich_text": [{"text": {"content": "Parent"}}]},
                    }
                ],
                "next_cursor": None,
            },
            {
                "results": [
                    {
                        "type": "paragraph",
                        "id": "grandchild-block",
                        "has_children": False,
                        "paragraph": {"rich_text": [{"text": {"content": "Child"}}]},
                    }
                ],
                "next_cursor": None,
            },
        ]
        job = _make_job(self.mock_client)
        item = IngestionItem(id="notion:page-1", source_ref="page-1")
        content = job.get_raw_content(item)
        self.assertIn("Parent", content)
        self.assertIn("Child", content)


class TestNotionGetItemName(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()

    def test_uses_title_from_cache(self):
        job = _make_job(self.mock_client)
        item = IngestionItem(id="notion:abc-123", source_ref="abc-123")
        item._metadata_cache["title"] = "My Page"
        self.assertEqual(job.get_item_name(item), "My_Page")

    def test_falls_back_to_source_ref(self):
        job = _make_job(self.mock_client)
        item = IngestionItem(id="notion:abc-123", source_ref="abc-123")
        self.assertEqual(job.get_item_name(item), "abc-123")

    def test_truncates_to_255(self):
        job = _make_job(self.mock_client)
        long_id = "a" * 300
        item = IngestionItem(id=f"notion:{long_id}", source_ref=long_id)
        self.assertLessEqual(len(job.get_item_name(item)), 255)


class TestNotionGetDocumentMetadata(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()

    def test_contains_required_fields(self):
        job = _make_job(self.mock_client)
        item = IngestionItem(
            id="notion:12345678-1234-1234-1234-123456789abc",
            source_ref="12345678-1234-1234-1234-123456789abc",
        )
        metadata = job.get_document_metadata(item=item, item_name="test", checksum="chk", version=1, last_modified=None)
        self.assertEqual(metadata["source"], "notion")
        self.assertEqual(metadata["id"], "12345678-1234-1234-1234-123456789abc")
        self.assertIn("notion.so", metadata["url"])
        self.assertNotIn("-", metadata["url"].replace("https://notion.so/", ""))
        self.assertEqual(metadata["source_name"], "test_notion")

    def test_public_url_included_when_present(self):
        job = _make_job(self.mock_client)
        item = IngestionItem(id="notion:abc-123", source_ref="abc-123")
        item._metadata_cache["public_url"] = "https://notion.so/public/abc123"
        metadata = job.get_document_metadata(
            item=item, item_name="abc-123", checksum="chk", version=1, last_modified=None
        )
        self.assertEqual(metadata["public_url"], "https://notion.so/public/abc123")


class TestNotionResolveUserName(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()

    def test_resolves_user_name(self):
        self.mock_client.users.retrieve.return_value = {"name": "Alice"}
        job = _make_job(self.mock_client)
        name = job._resolve_user_name("user-1")
        self.assertEqual(name, "Alice")
        self.mock_client.users.retrieve.assert_called_once_with(user_id="user-1")

    def test_returns_none_for_missing_user_id(self):
        job = _make_job(self.mock_client)
        self.assertIsNone(job._resolve_user_name(None))

    def test_caches_user_resolution(self):
        self.mock_client.users.retrieve.return_value = {"name": "Alice"}
        job = _make_job(self.mock_client)
        job._resolve_user_name("user-1")
        job._resolve_user_name("user-1")
        self.mock_client.users.retrieve.assert_called_once()

    def test_returns_none_on_api_error(self):
        self.mock_client.users.retrieve.side_effect = _api_error(403, "Forbidden", "restricted_resource")
        job = _make_job(self.mock_client)
        self.assertIsNone(job._resolve_user_name("user-1"))


if __name__ == "__main__":
    unittest.main()
