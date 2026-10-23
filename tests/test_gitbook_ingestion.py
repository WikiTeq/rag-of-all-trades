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
        page["_parent_id"] = parent
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
        parents = {i.source_ref["id"]: i.source_ref["_parent_id"] for i in items}
        self.assertEqual(parents, {"parent1": None, "child1": "parent1"})

    def test_list_items_skips_page_with_missing_id(self):
        self.mock_client.list_pages.return_value = [
            {"title": "No ID", "updatedAt": "2024-06-01T00:00:00.000Z", "urls": {"app": ""}},
            {"id": "p2", "title": "OK", "updatedAt": "2024-06-01T00:00:00.000Z", "urls": {"app": ""}},
        ]
        job = self._make_job(space_ids=["space1"])
        items = list(job.list_items())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "gitbook:space1:p2")

    def test_list_items_raises_on_space_discovery_error(self):
        self.mock_client.list_spaces.side_effect = Exception("auth failed")
        job = self._make_job()
        with self.assertRaisesRegex(Exception, "auth failed"):
            list(job.list_items())

    def test_list_items_raises_on_list_pages_error_after_yielding_earlier_space(self):
        self.mock_client.list_spaces.return_value = [{"id": "space1"}, {"id": "space2"}]
        self.mock_client.list_pages.side_effect = [
            [{"id": "p1", "title": "P", "updatedAt": "2024-06-01T00:00:00.000Z"}],
            Exception("forbidden"),
        ]
        job = self._make_job()
        yielded = []
        with self.assertRaisesRegex(Exception, "forbidden"):
            for item in job.list_items():
                yielded.append(item.id)
        self.assertEqual(yielded, ["gitbook:space1:p1"])

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

    def test_get_raw_content_strips_front_matter_and_keeps_one_title(self):
        item = _make_item(title="My Page")
        self.mock_client.get_page.return_value = {"markdown": "---\ndescription: About it\n---\n\n# My Page\n\nBody"}
        content = self._make_job().get_raw_content(item)
        self.assertEqual(content, "# My Page\n\nBody")

    def test_get_raw_content_front_matter_only_is_empty(self):
        self.mock_client.get_page.return_value = {"markdown": "---\ndescription: About it\n---\n"}
        with self.assertLogs("tasks.gitbook_ingestion", level="WARNING"):
            content = self._make_job().get_raw_content(_make_item())
        self.assertEqual(content, "")

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
    def _extra(self, **page_fields):
        job = GitBookIngestionJob(_make_config())
        item = _make_item(page_id="p1", space_id="s1")
        item.source_ref.update(page_fields)
        return job.get_extra_metadata(item=item, content="", metadata={})

    def test_core_fields(self):
        extra = self._extra(_parent_id="parent_p")
        self.assertEqual(extra["space_id"], "s1")
        self.assertEqual(extra["page_id"], "p1")
        self.assertEqual(extra["page_title"], "Test Page")
        self.assertEqual(extra["page_url"], "https://app.gitbook.com/s/s1/page/p1")
        self.assertEqual(extra["parent_page_id"], "parent_p")

    def test_no_parent(self):
        self.assertEqual(self._extra()["parent_page_id"], "")

    def test_page_title_prefers_link_title(self):
        self.assertEqual(self._extra(linkTitle="Short")["page_title"], "Short")

    def test_page_title_falls_back_to_title_when_link_title_empty_or_null(self):
        self.assertEqual(self._extra(linkTitle="")["page_title"], "Test Page")
        self.assertEqual(self._extra(linkTitle=None)["page_title"], "Test Page")

    def test_description_created_and_updated_dates(self):
        extra = self._extra(description="About this page", createdAt="2024-01-02T03:04:05.000Z")
        self.assertEqual(extra["description"], "About this page")
        self.assertEqual(extra["created_at"], "2024-01-02T03:04:05.000Z")
        self.assertEqual(extra["updated_at"], "2024-06-01T12:00:00.000Z")

    def test_missing_optional_fields_default_to_empty(self):
        extra = self._extra(description=None, createdAt=None, tags=None, urls=None)
        self.assertEqual(extra["description"], "")
        self.assertEqual(extra["created_at"], "")
        self.assertEqual(extra["tags"], [])
        self.assertEqual(extra["page_url"], "")

    def test_tags_are_slugs(self):
        tags = [
            {"tag": {"kind": "tag", "tag": "guide"}, "primary": True},
            {"tag": {"kind": "tag", "tag": "api", "space": "s2"}},
        ]
        self.assertEqual(self._extra(tags=tags)["tags"], ["guide", "api"])

    def test_malformed_tags_are_skipped(self):
        tags = [None, "x", {"primary": True}, {"tag": None}, {"tag": {"tag": ""}}, {"tag": {"tag": "ok"}}]
        self.assertEqual(self._extra(tags=tags)["tags"], ["ok"])


class TestGitBookPagination(unittest.TestCase):
    def setUp(self):
        patcher = patch("tasks.gitbook_ingestion.RetrySession")
        self.mock_session = patcher.start().return_value
        self.addCleanup(patcher.stop)

    def _client(self):
        from tasks.gitbook_ingestion import GitBookClient

        return GitBookClient("test-token")

    def _respond(self, *payloads):
        responses = []
        for payload in payloads:
            resp = MagicMock()
            resp.json.return_value = payload
            responses.append(resp)
        self.mock_session.get.side_effect = responses

    def _requests(self):
        return [
            (c.args[0].removeprefix("https://api.gitbook.com/v1"), c.kwargs["params"])
            for c in self.mock_session.get.call_args_list
        ]

    def test_list_orgs_follows_cursor(self):
        self._respond(
            {"items": [{"id": "org1"}], "next": {"page": "c1"}},
            {"items": [{"id": "org2"}], "next": None},
        )
        orgs = self._client().list_orgs()
        self.assertEqual([o["id"] for o in orgs], ["org1", "org2"])
        self.assertEqual(self._requests(), [("/orgs", None), ("/orgs", {"page": "c1"})])

    def test_list_spaces_paginates_orgs_and_spaces(self):
        self._respond(
            {"items": [{"id": "org1"}], "next": {"page": "o2"}},
            {"items": [{"id": "org2"}]},
            {"items": [{"id": "s1"}], "next": {"page": "sc1"}},
            {"items": [{"id": "s2"}]},
            {"items": [{"id": "s3"}]},
        )
        spaces = self._client().list_spaces()
        self.assertEqual([s["id"] for s in spaces], ["s1", "s2", "s3"])
        self.assertEqual(
            self._requests(),
            [
                ("/orgs", None),
                ("/orgs", {"page": "o2"}),
                ("/orgs/org1/spaces", None),
                ("/orgs/org1/spaces", {"page": "sc1"}),
                ("/orgs/org2/spaces", None),
            ],
        )

    def test_list_spaces_skips_org_without_id(self):
        self._respond({"items": [{"name": "no id"}]})
        self.assertEqual(self._client().list_spaces(), [])

    def test_cursor_is_sent_as_param_not_in_url(self):
        self._respond({"items": [], "next": {"page": "a&b=c#d"}}, {"items": []})
        self._client().list_orgs()
        self.assertEqual(self._requests()[1], ("/orgs", {"page": "a&b=c#d"}))

    def test_empty_page_with_next_cursor_continues(self):
        self._respond({"items": [], "next": {"page": "c1"}}, {"items": [{"id": "org1"}]})
        self.assertEqual(self._client().list_orgs(), [{"id": "org1"}])

    def test_invalid_next_cursor_raises(self):
        for bad in ({"page": ""}, {"page": 5}, "c1"):
            with self.subTest(next=bad):
                self._respond({"items": [], "next": bad})
                with self.assertRaisesRegex(ValueError, "invalid cursor"):
                    self._client().list_orgs()

    def test_response_without_items_list_raises(self):
        for bad in ({}, {"items": None}, {"items": "x"}):
            with self.subTest(payload=bad):
                self._respond(bad)
                with self.assertRaisesRegex(ValueError, "no `items` list"):
                    self._client().list_orgs()

    def test_list_pages_without_pages_list_raises(self):
        for bad in ({}, {"pages": None}):
            with self.subTest(payload=bad):
                self._respond(bad)
                with self.assertRaisesRegex(ValueError, "no `pages` list"):
                    self._client().list_pages("space1")

    def test_list_pages_returns_page_tree(self):
        self._respond({"pages": [{"id": "p1"}]})
        self.assertEqual(self._client().list_pages("space1"), [{"id": "p1"}])

    def test_repeated_cursor_raises(self):
        self._respond(
            {"items": [], "next": {"page": "a"}},
            {"items": [], "next": {"page": "b"}},
            {"items": [], "next": {"page": "a"}},
        )
        with self.assertRaisesRegex(ValueError, "repeated cursor"):
            self._client().list_orgs()

    def test_later_page_http_error_propagates(self):
        ok = MagicMock()
        ok.json.return_value = {"items": [{"id": "org1"}], "next": {"page": "c1"}}
        bad = MagicMock()
        bad.raise_for_status.side_effect = RuntimeError("403")
        self.mock_session.get.side_effect = [ok, bad]
        with self.assertRaisesRegex(RuntimeError, "403"):
            self._client().list_orgs()


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
        with self.assertRaises(Exception):
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
        result = list(_flatten_pages([{"id": "1"}, {"id": "2"}]))
        self.assertEqual([(p["id"], parent) for p, parent in result], [("1", None), ("2", None)])

    def test_nested_pages_carry_parent_id(self):
        pages = [{"id": "1", "pages": [{"id": "1a"}, {"id": "1b", "pages": [{"id": "1b1"}]}]}, {"id": "2"}]
        result = [(p["id"], parent) for p, parent in _flatten_pages(pages)]
        self.assertEqual(result, [("1", None), ("1a", "1"), ("1b", "1"), ("1b1", "1b"), ("2", None)])


if __name__ == "__main__":
    unittest.main()
