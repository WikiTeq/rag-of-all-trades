import unittest
from unittest.mock import MagicMock, patch

from tasks.gitbook_ingestion import (
    GitBookIngestionJob,
    _flatten_pages,
)
from tasks.helper_classes.ingestion_item import IngestionItem


def _make_config(**kwargs):
    cfg = {"api_token": "test-token"}
    cfg.update(kwargs)
    return {"name": "gitbook-test", "config": cfg}


def _make_page(
    page_id="page1", space_id="space1", title="Test Page", updated_at="2024-06-01T12:00:00.000Z", parent=None
):
    page = {
        "id": page_id,
        "title": title,
        "updatedAt": updated_at,
        "urls": {"app": f"https://app.gitbook.com/s/{space_id}/page/{page_id}"},
        "_space_id": space_id,
    }
    if parent:
        page["parent"] = parent
    return page


def _make_item(page_id="page1", space_id="space1", **kwargs):
    page = _make_page(page_id=page_id, space_id=space_id, **kwargs)
    return IngestionItem(id=f"gitbook:{space_id}:{page_id}", source_ref=page)


class TestGitBookIngestionJobInit(unittest.TestCase):
    def test_source_type(self):
        job = GitBookIngestionJob(_make_config())
        self.assertEqual(job.source_type, "gitbook")

    def test_missing_api_token_raises(self):
        with self.assertRaises(ValueError):
            GitBookIngestionJob({"name": "x", "config": {}})

    def test_empty_api_token_raises(self):
        with self.assertRaises(ValueError):
            GitBookIngestionJob({"name": "x", "config": {"api_token": "  "}})

    def test_space_ids_default_empty(self):
        job = GitBookIngestionJob(_make_config())
        self.assertEqual(job.space_ids, [])

    def test_space_ids_parsed_from_list(self):
        job = GitBookIngestionJob(_make_config(space_ids=["space1", "space2"]))
        self.assertEqual(job.space_ids, ["space1", "space2"])

    def test_space_ids_parsed_from_string(self):
        job = GitBookIngestionJob(_make_config(space_ids="space1,space2"))
        self.assertEqual(job.space_ids, ["space1", "space2"])


class TestGitBookListItems(unittest.TestCase):
    def setUp(self):
        self.patcher = patch("tasks.gitbook_ingestion.GitBookClient")
        self.mock_client_class = self.patcher.start()
        self.mock_client = MagicMock()
        self.mock_client_class.return_value = self.mock_client

    def tearDown(self):
        self.patcher.stop()

    def _make_job(self, **kwargs):
        return GitBookIngestionJob(_make_config(**kwargs))

    def test_list_items_uses_configured_space_ids(self):
        self.mock_client.list_pages.return_value = [
            {"id": "p1", "title": "Page 1", "updatedAt": "2024-06-01T00:00:00.000Z", "urls": {"app": ""}}
        ]
        job = self._make_job(space_ids=["space1"])
        items = list(job.list_items())
        self.mock_client.list_spaces.assert_not_called()
        self.mock_client.list_pages.assert_called_once_with("space1")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "gitbook:space1:p1")

    def test_list_items_discovers_spaces_when_none_configured(self):
        self.mock_client.list_spaces.return_value = [{"id": "space1"}, {"id": "space2"}]
        self.mock_client.list_pages.return_value = []
        job = self._make_job()
        list(job.list_items())
        self.mock_client.list_spaces.assert_called_once()
        self.assertEqual(self.mock_client.list_pages.call_count, 2)

    def test_list_items_flattens_nested_pages(self):
        self.mock_client.list_pages.return_value = [
            {
                "id": "parent1",
                "title": "Parent",
                "updatedAt": "2024-06-01T00:00:00.000Z",
                "urls": {"app": ""},
                "pages": [
                    {"id": "child1", "title": "Child", "updatedAt": "2024-06-02T00:00:00.000Z", "urls": {"app": ""}}
                ],
            }
        ]
        job = self._make_job(space_ids=["space1"])
        items = list(job.list_items())
        ids = {i.id for i in items}
        self.assertIn("gitbook:space1:parent1", ids)
        self.assertIn("gitbook:space1:child1", ids)

    def test_list_items_skips_page_with_missing_id(self):
        self.mock_client.list_pages.return_value = [
            {"title": "No ID", "updatedAt": "2024-06-01T00:00:00.000Z", "urls": {"app": ""}},
            {"id": "p2", "title": "OK", "updatedAt": "2024-06-01T00:00:00.000Z", "urls": {"app": ""}},
        ]
        job = self._make_job(space_ids=["space1"])
        items = list(job.list_items())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "gitbook:space1:p2")

    def test_list_items_returns_empty_on_api_error(self):
        self.mock_client.list_spaces.side_effect = Exception("auth failed")
        job = self._make_job()
        items = list(job.list_items())
        self.assertEqual(items, [])

    def test_list_items_skips_space_on_list_pages_error(self):
        self.mock_client.list_spaces.return_value = [{"id": "space1"}, {"id": "space2"}]
        self.mock_client.list_pages.side_effect = [Exception("forbidden"), []]
        job = self._make_job()
        items = list(job.list_items())
        self.assertEqual(items, [])

    def test_list_items_last_modified_parsed(self):
        self.mock_client.list_pages.return_value = [
            {"id": "p1", "title": "P", "updatedAt": "2024-06-15T10:30:00.000Z", "urls": {"app": ""}}
        ]
        job = self._make_job(space_ids=["space1"])
        items = list(job.list_items())
        self.assertIsNotNone(items[0].last_modified)
        self.assertEqual(items[0].last_modified.year, 2024)
        self.assertEqual(items[0].last_modified.month, 6)


class TestGitBookGetRawContent(unittest.TestCase):
    def setUp(self):
        self.patcher = patch("tasks.gitbook_ingestion.GitBookClient")
        self.mock_client_class = self.patcher.start()
        self.mock_client = MagicMock()
        self.mock_client_class.return_value = self.mock_client

    def tearDown(self):
        self.patcher.stop()

    def _make_job(self):
        return GitBookIngestionJob(_make_config())

    def test_get_raw_content_returns_markdown(self):
        item = _make_item()
        self.mock_client.get_page.return_value = {"markdown": "Hello world\n"}
        job = self._make_job()
        content = job.get_raw_content(item)
        self.assertIn("Hello world", content)

    def test_get_raw_content_prepends_title(self):
        item = _make_item(title="My Page")
        self.mock_client.get_page.return_value = {"markdown": "Body text"}
        job = self._make_job()
        content = job.get_raw_content(item)
        self.assertTrue(content.startswith("# My Page"))

    def test_get_raw_content_does_not_duplicate_title(self):
        item = _make_item(title="My Page")
        self.mock_client.get_page.return_value = {"markdown": "# My Page\n\nBody"}
        job = self._make_job()
        content = job.get_raw_content(item)
        self.assertEqual(content.count("My Page"), 1)

    def test_get_raw_content_empty_warns_and_returns_empty(self):
        item = _make_item()
        self.mock_client.get_page.return_value = {"markdown": "  \n"}
        job = self._make_job()
        with self.assertLogs("tasks.gitbook_ingestion", level="WARNING") as cm:
            content = job.get_raw_content(item)
        self.assertEqual(content, "")
        self.assertTrue(any("Empty content" in line for line in cm.output))

    def test_get_raw_content_caches_url_and_title(self):
        item = _make_item(title="Cached Page")
        self.mock_client.get_page.return_value = {"markdown": "text"}
        job = self._make_job()
        job.get_raw_content(item)
        self.assertEqual(item._metadata_cache["title"], "Cached Page")
        self.assertIn("page1", item._metadata_cache["url"])

    def test_get_raw_content_returns_empty_when_markdown_missing_or_not_a_string(self):
        job = self._make_job()
        for detail in ({}, {"markdown": None}, {"markdown": 5}):
            self.mock_client.get_page.return_value = detail
            self.assertEqual(job.get_raw_content(_make_item()), "")

    def test_get_raw_content_returns_empty_on_api_error(self):
        item = _make_item()
        self.mock_client.get_page.side_effect = Exception("network error")
        job = self._make_job()
        content = job.get_raw_content(item)
        self.assertEqual(content, "")


class TestGitBookGetItemName(unittest.TestCase):
    def test_get_item_name_stable_format(self):
        job = GitBookIngestionJob(_make_config())
        item = _make_item(page_id="abc123", space_id="spaceXYZ")
        name = job.get_item_name(item)
        self.assertIn("gitbook", name)
        self.assertIn("spaceXYZ", name)
        self.assertIn("abc123", name)
        self.assertLessEqual(len(name), 255)

    def test_get_item_name_max_length(self):
        job = GitBookIngestionJob(_make_config())
        item = _make_item(page_id="x" * 200, space_id="y" * 200)
        name = job.get_item_name(item)
        self.assertLessEqual(len(name), 255)


class TestGitBookGetItemChecksum(unittest.TestCase):
    def test_checksum_includes_updated_at(self):
        job = GitBookIngestionJob(_make_config())
        item = _make_item(updated_at="2024-06-01T12:00:00.000Z")
        checksum = job.get_item_checksum(item)
        self.assertIsNotNone(checksum)
        self.assertEqual(checksum, "space1:page1:2024-06-01T12:00:00.000Z")

    def test_checksum_none_when_no_updated_at(self):
        job = GitBookIngestionJob(_make_config())
        page = _make_page()
        del page["updatedAt"]
        item = IngestionItem(id="gitbook:space1:page1", source_ref=page)
        self.assertIsNone(job.get_item_checksum(item))


class TestGitBookGetExtraMetadata(unittest.TestCase):
    def test_extra_metadata_fields(self):
        job = GitBookIngestionJob(_make_config())
        item = _make_item(page_id="p1", space_id="s1", parent="parent_p")
        object.__setattr__(item, "_metadata_cache", {"title": "Page Title", "url": "https://app.gitbook.com/p1"})
        extra = job.get_extra_metadata(item=item, content="", metadata={})
        self.assertEqual(extra["space_id"], "s1")
        self.assertEqual(extra["page_id"], "p1")
        self.assertEqual(extra["page_title"], "Page Title")
        self.assertEqual(extra["page_url"], "https://app.gitbook.com/p1")
        self.assertEqual(extra["parent_page_id"], "parent_p")

    def test_extra_metadata_no_parent(self):
        job = GitBookIngestionJob(_make_config())
        item = _make_item()
        object.__setattr__(item, "_metadata_cache", {"title": "", "url": ""})
        extra = job.get_extra_metadata(item=item, content="", metadata={})
        self.assertEqual(extra["parent_page_id"], "")


class TestListSpacesPagination(unittest.TestCase):
    def setUp(self):
        patcher = patch("tasks.gitbook_ingestion.RetrySession")
        self.mock_session_cls = patcher.start()
        self.mock_session = MagicMock()
        self.mock_session_cls.return_value = self.mock_session
        self.addCleanup(patcher.stop)

    def _make_client(self):
        from tasks.gitbook_ingestion import GitBookClient

        return GitBookClient("test-token")

    def test_list_spaces_paginates(self):
        orgs_resp = MagicMock()
        orgs_resp.json.return_value = {"items": [{"id": "org1"}]}
        page1_resp = MagicMock()
        page1_resp.json.return_value = {"items": [{"id": "space1"}], "next": {"page": "cursor1"}}
        page2_resp = MagicMock()
        page2_resp.json.return_value = {"items": [{"id": "space2"}], "next": None}
        self.mock_session.get.side_effect = [orgs_resp, page1_resp, page2_resp]

        client = self._make_client()
        spaces = client.list_spaces()
        self.assertEqual([s["id"] for s in spaces], ["space1", "space2"])

    def test_list_spaces_no_next(self):
        orgs_resp = MagicMock()
        orgs_resp.json.return_value = {"items": [{"id": "org1"}]}
        spaces_resp = MagicMock()
        spaces_resp.json.return_value = {"items": [{"id": "spaceA"}]}
        self.mock_session.get.side_effect = [orgs_resp, spaces_resp]

        client = self._make_client()
        spaces = client.list_spaces()
        self.assertEqual(len(spaces), 1)

    def test_list_spaces_multiple_orgs(self):
        orgs_resp = MagicMock()
        orgs_resp.json.return_value = {"items": [{"id": "org1"}, {"id": "org2"}]}
        spaces1_resp = MagicMock()
        spaces1_resp.json.return_value = {"items": [{"id": "s1"}]}
        spaces2_resp = MagicMock()
        spaces2_resp.json.return_value = {"items": [{"id": "s2"}]}
        self.mock_session.get.side_effect = [orgs_resp, spaces1_resp, spaces2_resp]

        client = self._make_client()
        spaces = client.list_spaces()
        self.assertEqual({s["id"] for s in spaces}, {"s1", "s2"})


class TestGetPageRequest(unittest.TestCase):
    def test_get_page_requests_markdown_format(self):
        with patch("tasks.gitbook_ingestion.RetrySession") as session_cls:
            session = session_cls.return_value
            session.get.return_value.json.return_value = {"markdown": "# T"}
            from tasks.gitbook_ingestion import GitBookClient

            result = GitBookClient("test-token").get_page("space1", "page1")
        self.assertEqual(result, {"markdown": "# T"})
        _, kwargs = session.get.call_args
        self.assertEqual(kwargs["params"], {"format": "markdown"})
        self.assertTrue(session.get.call_args.args[0].endswith("/spaces/space1/content/page/page1"))


class TestListItemsSessionLeak(unittest.TestCase):
    def setUp(self):
        patcher = patch("tasks.gitbook_ingestion.GitBookClient")
        self.mock_client_cls = patcher.start()
        self.mock_client = MagicMock()
        self.mock_client_cls.return_value = self.mock_client
        self.addCleanup(patcher.stop)

    def test_client_built_once_with_api_token(self):
        job = GitBookIngestionJob(_make_config())
        self.mock_client.list_spaces.return_value = []
        list(job.list_items())
        job.get_raw_content(_make_item())
        self.mock_client_cls.assert_called_once_with("test-token")

    def test_get_raw_content_does_not_close_client(self):
        self.mock_client.get_page.return_value = {"markdown": ""}
        job = GitBookIngestionJob(_make_config())
        job.get_raw_content(_make_item())
        self.mock_client.close.assert_not_called()

    def test_client_closed_on_resolve_failure(self):
        self.mock_client.list_spaces.side_effect = Exception("auth failed")
        job = GitBookIngestionJob(_make_config())
        list(job.list_items())
        self.mock_client.close.assert_called_once()

    def test_client_closed_on_success(self):
        self.mock_client.list_spaces.return_value = []
        job = GitBookIngestionJob(_make_config(space_ids=["s1"]))
        self.mock_client.list_pages.return_value = []
        list(job.list_items())
        self.mock_client.close.assert_called_once()


class TestFlattenPages(unittest.TestCase):
    def test_flat_list(self):
        pages = [{"id": "1"}, {"id": "2"}]
        result = list(_flatten_pages(pages))
        self.assertEqual(len(result), 2)

    def test_nested_pages(self):
        pages = [{"id": "1", "pages": [{"id": "1a"}, {"id": "1b", "pages": [{"id": "1b1"}]}]}, {"id": "2"}]
        result = list(_flatten_pages(pages))
        ids = [p["id"] for p in result]
        self.assertEqual(ids, ["1", "1a", "1b", "1b1", "2"])


if __name__ == "__main__":
    unittest.main()
