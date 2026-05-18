import unittest
from unittest.mock import MagicMock, patch

from tasks.gitbook_ingestion import (
    GitBookIngestionJob,
    GitBookMarkdownConverter,
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
        self.mock_client.get_page.return_value = {
            "document": {"nodes": [{"type": "paragraph", "nodes": [{"type": "text", "text": "Hello world"}]}]}
        }
        job = self._make_job()
        content = job.get_raw_content(item)
        self.assertIn("Hello world", content)

    def test_get_raw_content_prepends_title(self):
        item = _make_item(title="My Page")
        self.mock_client.get_page.return_value = {
            "document": {"nodes": [{"type": "paragraph", "nodes": [{"type": "text", "text": "Body text"}]}]}
        }
        job = self._make_job()
        content = job.get_raw_content(item)
        self.assertTrue(content.startswith("# My Page"))

    def test_get_raw_content_does_not_duplicate_title(self):
        item = _make_item(title="My Page")
        self.mock_client.get_page.return_value = {
            "document": {
                "nodes": [
                    {"type": "heading-1", "nodes": [{"type": "text", "text": "My Page"}]},
                    {"type": "paragraph", "nodes": [{"type": "text", "text": "Body"}]},
                ]
            }
        }
        job = self._make_job()
        content = job.get_raw_content(item)
        self.assertEqual(content.count("My Page"), 1)

    def test_get_raw_content_empty_warns_and_returns_empty(self):
        item = _make_item()
        self.mock_client.get_page.return_value = {"document": {"nodes": []}}
        job = self._make_job()
        with self.assertLogs("tasks.gitbook_ingestion", level="WARNING") as cm:
            content = job.get_raw_content(item)
        self.assertEqual(content, "")
        self.assertTrue(any("Empty content" in line for line in cm.output))

    def test_get_raw_content_caches_url_and_title(self):
        item = _make_item(title="Cached Page")
        self.mock_client.get_page.return_value = {
            "document": {"nodes": [{"type": "paragraph", "nodes": [{"type": "text", "text": "text"}]}]}
        }
        job = self._make_job()
        job.get_raw_content(item)
        self.assertEqual(item._metadata_cache["title"], "Cached Page")
        self.assertIn("page1", item._metadata_cache["url"])

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
        self.assertIn("2024-06-01T12:00:00.000Z", checksum)

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


class TestExtractMarkdown(unittest.TestCase):
    def test_paragraph(self):
        doc = {"nodes": [{"type": "paragraph", "nodes": [{"type": "text", "text": "Hello"}]}]}
        self.assertIn("Hello", GitBookMarkdownConverter.extract(doc))

    def test_heading(self):
        doc = {"nodes": [{"type": "heading-2", "nodes": [{"type": "text", "text": "Title"}]}]}
        result = GitBookMarkdownConverter.extract(doc)
        self.assertIn("## Title", result)

    def test_code_block(self):
        doc = {"nodes": [{"type": "code", "data": {"syntax": "python"}, "nodes": [{"type": "text", "text": "x = 1"}]}]}
        result = GitBookMarkdownConverter.extract(doc)
        self.assertIn("```python", result)
        self.assertIn("x = 1", result)

    def test_empty_document(self):
        self.assertEqual(GitBookMarkdownConverter.extract({}), "")
        self.assertEqual(GitBookMarkdownConverter.extract({"nodes": []}), "")

    def test_bold_text(self):
        doc = {
            "nodes": [{"type": "paragraph", "nodes": [{"type": "text", "text": "bold", "marks": [{"type": "bold"}]}]}]
        }
        result = GitBookMarkdownConverter.extract(doc)
        self.assertIn("**bold**", result)


class TestExtractMarkdownExtended(unittest.TestCase):
    def test_blockquote_prefixed(self):
        doc = {
            "nodes": [
                {
                    "type": "blockquote",
                    "nodes": [
                        {"type": "paragraph", "nodes": [{"type": "text", "text": "line one"}]},
                        {"type": "paragraph", "nodes": [{"type": "text", "text": "line two"}]},
                    ],
                }
            ]
        }
        result = GitBookMarkdownConverter.extract(doc)
        for line in result.strip().splitlines():
            self.assertTrue(line.startswith(">"), f"Expected '>' prefix, got: {line!r}")

    def test_hint_prefixed(self):
        doc = {
            "nodes": [
                {
                    "type": "hint",
                    "nodes": [
                        {"type": "paragraph", "nodes": [{"type": "text", "text": "note line one"}]},
                        {"type": "paragraph", "nodes": [{"type": "text", "text": "note line two"}]},
                    ],
                }
            ]
        }
        result = GitBookMarkdownConverter.extract(doc)
        for line in result.strip().splitlines():
            self.assertTrue(line.startswith(">"), f"Expected '>' prefix, got: {line!r}")

    def test_unordered_list(self):
        doc = {
            "nodes": [
                {
                    "type": "list-unordered",
                    "nodes": [
                        {
                            "type": "list-item",
                            "nodes": [{"type": "paragraph", "nodes": [{"type": "text", "text": "item A"}]}],
                        },
                        {
                            "type": "list-item",
                            "nodes": [{"type": "paragraph", "nodes": [{"type": "text", "text": "item B"}]}],
                        },
                    ],
                }
            ]
        }
        result = GitBookMarkdownConverter.extract(doc)
        self.assertIn("- item A", result)
        self.assertIn("- item B", result)

    def test_ordered_list(self):
        doc = {
            "nodes": [
                {
                    "type": "list-ordered",
                    "nodes": [
                        {
                            "type": "list-item",
                            "nodes": [{"type": "paragraph", "nodes": [{"type": "text", "text": "first"}]}],
                        },
                        {
                            "type": "list-item",
                            "nodes": [{"type": "paragraph", "nodes": [{"type": "text", "text": "second"}]}],
                        },
                    ],
                }
            ]
        }
        result = GitBookMarkdownConverter.extract(doc)
        self.assertIn("1. first", result)
        self.assertIn("2. second", result)

    def test_text_node_with_leaves(self):
        doc = {
            "nodes": [
                {
                    "type": "paragraph",
                    "nodes": [
                        {
                            "object": "text",
                            "leaves": [
                                {"text": "plain", "marks": []},
                                {"text": "bold", "marks": [{"type": "bold"}]},
                            ],
                        }
                    ],
                }
            ]
        }
        result = GitBookMarkdownConverter.extract(doc)
        self.assertIn("plain", result)
        self.assertIn("**bold**", result)

    def test_link(self):
        doc = {
            "nodes": [
                {
                    "type": "paragraph",
                    "nodes": [
                        {
                            "type": "link",
                            "data": {"url": "https://example.com"},
                            "nodes": [{"type": "text", "text": "click"}],
                        }
                    ],
                }
            ]
        }
        result = GitBookMarkdownConverter.extract(doc)
        self.assertIn("[click](https://example.com)", result)

    def test_image(self):
        doc = {"nodes": [{"type": "image", "data": {"src": "https://img.example.com/a.png", "alt": "logo"}}]}
        result = GitBookMarkdownConverter.extract(doc)
        self.assertIn("![logo](https://img.example.com/a.png)", result)

    def test_table(self):
        doc = {
            "nodes": [
                {
                    "type": "table",
                    "nodes": [
                        {
                            "type": "table-row",
                            "nodes": [
                                {"type": "table-cell", "nodes": [{"type": "text", "text": "H1"}]},
                                {"type": "table-cell", "nodes": [{"type": "text", "text": "H2"}]},
                            ],
                        },
                        {
                            "type": "table-row",
                            "nodes": [
                                {"type": "table-cell", "nodes": [{"type": "text", "text": "A"}]},
                                {"type": "table-cell", "nodes": [{"type": "text", "text": "B"}]},
                            ],
                        },
                    ],
                }
            ]
        }
        result = GitBookMarkdownConverter.extract(doc)
        self.assertIn("| H1 | H2 |", result)
        self.assertIn("| A | B |", result)
        self.assertIn("---", result)


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


class TestListItemsSessionLeak(unittest.TestCase):
    def setUp(self):
        patcher = patch("tasks.gitbook_ingestion.GitBookClient")
        self.mock_client_cls = patcher.start()
        self.mock_client = MagicMock()
        self.mock_client_cls.return_value = self.mock_client
        self.addCleanup(patcher.stop)

    def test_client_closed_on_resolve_failure(self):
        self.mock_client._resolve_space_ids = MagicMock(side_effect=Exception("auth failed"))
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
