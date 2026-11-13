import json
import unittest
from unittest.mock import Mock, patch

import requests

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.wikijs_ingestion import WikiJsClient, WikiJsIngestionJob
from utils.graphql import GraphQLError


def _make_config(**kwargs):
    cfg = {
        "base_url": "https://wiki.example.com",
        "api_token": "test-token",
    }
    cfg.update(kwargs)
    return {"name": "wikijs-test", "config": cfg}


def _make_page(
    page_id=1,
    path="/test/page",
    title="Test Page",
    updated_at="2024-06-01T12:00:00.000Z",
    is_published=True,
    locale="en",
    tags=None,
):
    return {
        "id": page_id,
        "path": path,
        "locale": locale,
        "title": title,
        "updatedAt": updated_at,
        "isPublished": is_published,
        "tags": list(tags or []),
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


def _response(status=200, body=None, headers=None):
    resp = requests.Response()
    resp.status_code = status
    resp._content = json.dumps(body if body is not None else {}).encode()
    resp.raw = Mock()
    resp.headers.update(headers or {})
    return resp


class TestWikiJsClient(unittest.TestCase):
    def setUp(self):
        sleep_patcher = patch("utils.http.time.sleep")
        self.mock_sleep = sleep_patcher.start()
        self.addCleanup(sleep_patcher.stop)
        self.client = WikiJsClient("https://wiki.example.com/", "test-token")
        request_patcher = patch.object(self.client._session._session, "request")
        self.mock_request = request_patcher.start()
        self.addCleanup(request_patcher.stop)

    def _list_ok(self, pages=None):
        return _response(200, {"data": {"pages": {"list": pages if pages is not None else [_make_page()]}}})

    def _single_ok(self, page=None):
        return _response(200, {"data": {"pages": {"single": page if page is not None else _make_page_detail()}}})

    def test_base_url_trailing_slash_is_stripped(self):
        self.assertEqual(self.client.base_url, "https://wiki.example.com")

    def test_list_pages_sends_endpoint_auth_header_and_empty_variables(self):
        self.mock_request.return_value = self._list_ok()

        pages = self.client.list_pages()

        self.assertEqual(len(pages), 1)
        args, kwargs = self.mock_request.call_args
        self.assertEqual(args, ("POST", "https://wiki.example.com/graphql"))
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer test-token")
        self.assertEqual(kwargs["json"]["variables"], {})

    def test_list_pages_sends_tags_and_locale_filters(self):
        self.mock_request.return_value = self._list_ok()

        self.client.list_pages(tags=["a", "b"], locale="fr")

        self.assertEqual(self.mock_request.call_args.kwargs["json"]["variables"], {"tags": ["a", "b"], "locale": "fr"})

    def test_list_pages_omits_empty_filters(self):
        self.mock_request.return_value = self._list_ok()

        self.client.list_pages(tags=[], locale="")

        self.assertEqual(self.mock_request.call_args.kwargs["json"]["variables"], {})

    def test_list_pages_returns_empty_list_when_shape_is_unexpected(self):
        self.mock_request.return_value = _response(200, {"data": {"pages": {"list": "oops"}}})

        self.assertEqual(self.client.list_pages(), [])

    def test_list_pages_returns_empty_list_when_data_is_missing(self):
        self.mock_request.return_value = _response(200, {"data": {}})

        self.assertEqual(self.client.list_pages(), [])

    def test_list_pages_raises_graphql_error(self):
        self.mock_request.return_value = _response(200, {"errors": [{"message": "Forbidden"}]})

        with self.assertRaisesRegex(GraphQLError, "Forbidden"):
            self.client.list_pages()

    def test_list_pages_raises_graphql_error_on_non_json_body(self):
        resp = _response(200)
        resp._content = b"<html>proxy error</html>"
        self.mock_request.return_value = resp

        with self.assertRaisesRegex(GraphQLError, "Non-JSON"):
            self.client.list_pages()

    def test_list_pages_retries_on_server_error_then_succeeds(self):
        self.mock_request.side_effect = [_response(503), _response(502), self._list_ok()]

        pages = self.client.list_pages()

        self.assertEqual(len(pages), 1)
        self.assertEqual(self.mock_request.call_count, 3)
        self.assertEqual([c.args[0] for c in self.mock_sleep.call_args_list], [1, 2])

    def test_list_pages_retries_on_rate_limit_and_honours_retry_after(self):
        self.mock_request.side_effect = [_response(429, headers={"Retry-After": "7"}), self._list_ok()]

        self.client.list_pages()

        self.assertEqual(self.mock_request.call_count, 2)
        self.mock_sleep.assert_called_once_with(7)

    def test_list_pages_retries_on_network_error(self):
        self.mock_request.side_effect = [requests.ConnectionError("boom"), self._list_ok()]

        pages = self.client.list_pages()

        self.assertEqual(len(pages), 1)
        self.assertEqual(self.mock_request.call_count, 2)

    def test_list_pages_raises_network_error_after_retries_exhausted(self):
        self.mock_request.side_effect = requests.ConnectionError("boom")

        with self.assertRaises(requests.ConnectionError):
            self.client.list_pages()

        self.assertEqual(self.mock_request.call_count, self.client._session.max_retries + 1)

    def test_list_pages_raises_http_error_after_server_errors_exhausted(self):
        self.mock_request.return_value = _response(503)

        with self.assertRaises(requests.HTTPError):
            self.client.list_pages()

        self.assertEqual(self.mock_request.call_count, self.client._session.max_retries + 1)

    def test_list_pages_does_not_retry_client_errors(self):
        self.mock_request.return_value = _response(401)

        with self.assertRaises(requests.HTTPError):
            self.client.list_pages()

        self.assertEqual(self.mock_request.call_count, 1)
        self.mock_sleep.assert_not_called()

    def test_get_page_sends_page_id(self):
        self.mock_request.return_value = self._single_ok(_make_page_detail(page_id=5))

        page = self.client.get_page(5)

        self.assertEqual(page["id"], 5)
        self.assertEqual(self.mock_request.call_args.kwargs["json"]["variables"], {"id": 5})

    def test_get_page_returns_empty_dict_when_shape_is_unexpected(self):
        self.mock_request.return_value = _response(200, {"data": {"pages": {"single": None}}})

        self.assertEqual(self.client.get_page(5), {})

    def test_get_page_returns_empty_dict_when_data_is_missing(self):
        self.mock_request.return_value = _response(200, {"data": {}})

        self.assertEqual(self.client.get_page(5), {})

    def test_get_page_raises_graphql_error_when_token_is_forbidden(self):
        self.mock_request.return_value = _response(
            200, {"errors": [{"message": "You are not authorized to view this page."}]}
        )

        with self.assertRaisesRegex(GraphQLError, "not authorized"):
            self.client.get_page(5)

    def test_get_page_retries_on_server_error_then_succeeds(self):
        self.mock_request.side_effect = [_response(502), self._single_ok()]

        page = self.client.get_page(1)

        self.assertEqual(page["content"], "Some content")
        self.assertEqual(self.mock_request.call_count, 2)

    def test_get_page_does_not_retry_client_errors(self):
        self.mock_request.return_value = _response(403)

        with self.assertRaises(requests.HTTPError):
            self.client.get_page(1)

        self.assertEqual(self.mock_request.call_count, 1)


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

    def test_get_raw_content_logs_required_scopes_and_reraises_graphql_error(self):
        item = IngestionItem(id="wikijs:1", source_ref=_make_page(1, "/test", "My Page"))
        self.mock_client.get_page.side_effect = GraphQLError("You are not authorized to view this page.")

        job = self._make_job()
        with self.assertLogs("tasks.wikijs_ingestion", level="ERROR") as logs:
            with self.assertRaises(GraphQLError):
                job.get_raw_content(item)

        message = logs.output[0]
        self.assertIn("Cannot read page 1", message)
        self.assertIn("not authorized", message)
        self.assertIn("read:pages", message)
        self.assertIn("read:source", message)
        self.assertIn("manage:pages", message)

    def test_get_raw_content_empty_content_warning_lists_required_scopes(self):
        item = IngestionItem(id="wikijs:1", source_ref=_make_page(1, "/test", "My Page"))
        self.mock_client.get_page.return_value = _make_page_detail(1, content="   ")

        job = self._make_job()
        with self.assertLogs("tasks.wikijs_ingestion", level="WARNING") as logs:
            content = job.get_raw_content(item)

        self.assertEqual(content, "")
        self.assertIn("read:source", logs.output[0])
        self.assertIn("manage:pages", logs.output[0])

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
        page = _make_page(1, "/eng/setup", "Setup Guide", locale="en")
        item = IngestionItem(id="wikijs:1", source_ref=page)
        detail = {**_make_page_detail(1, path="/eng/setup", title="Setup Guide"), "locale": "en"}
        self.mock_client.get_page.return_value = detail

        job = self._make_job()
        job.get_raw_content(item)

        self.assertEqual(item._metadata_cache["title"], "Setup Guide")
        self.assertIn("en/eng/setup", item._metadata_cache["url"])

    def test_get_item_name_uses_only_stable_id(self):
        page = _make_page(42, "/test", "My Page")
        item = IngestionItem(id="wikijs:42", source_ref=page)
        job = self._make_job()
        name = job.get_item_name(item)
        self.assertEqual(name, "wikijs-42")
        self.assertLessEqual(len(name), 255)

    def test_get_extra_metadata_fields(self):
        page = _make_page(7, "/eng/guide", "Guide", "2024-06-01T12:00:00.000Z", locale="fr", tags=["public", "docs"])
        item = IngestionItem(id="wikijs:7", source_ref=page)
        object.__setattr__(item, "_metadata_cache", {"title": "Guide", "url": "https://wiki.example.com/fr/eng/guide"})

        job = self._make_job()
        extra = job.get_extra_metadata(item=item, content="", metadata={})

        self.assertEqual(extra["page_id"], "7")
        self.assertEqual(extra["path"], "/eng/guide")
        self.assertEqual(extra["locale"], "fr")
        self.assertEqual(extra["title"], "Guide")
        self.assertEqual(extra["url"], "https://wiki.example.com/fr/eng/guide")
        self.assertIn("2024", extra["updated_at"])
        self.assertEqual(extra["tags"], "public,docs")
        self.assertTrue(extra["is_published"])


if __name__ == "__main__":
    unittest.main()
