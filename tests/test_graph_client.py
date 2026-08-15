import unittest
from unittest.mock import MagicMock, patch

from utils.graph_client import GraphClient, GraphItemNotFoundError


def _make_client(**kwargs) -> GraphClient:
    defaults = {"client_id": "cid", "client_secret": "csecret", "tenant_id": "tid"}
    defaults.update(kwargs)
    return GraphClient(**defaults)


class TestGraphClientAuth(unittest.TestCase):
    def setUp(self):
        self.msal_patcher = patch("utils.graph_client.msal.ConfidentialClientApplication")
        self.mock_msal_cls = self.msal_patcher.start()
        self.mock_msal_app = MagicMock()
        self.mock_msal_cls.return_value = self.mock_msal_app
        self.mock_msal_app.acquire_token_for_client.return_value = {"access_token": "tok-1"}

    def tearDown(self):
        self.msal_patcher.stop()

    def test_builds_msal_app_with_correct_authority(self):
        _make_client(tenant_id="my-tenant")
        self.mock_msal_cls.assert_called_once_with(
            client_id="cid",
            client_credential="csecret",
            authority="https://login.microsoftonline.com/my-tenant",
        )

    def test_get_access_token_returns_token(self):
        client = _make_client()
        token = client._get_access_token()
        self.assertEqual(token, "tok-1")
        self.mock_msal_app.acquire_token_for_client.assert_called_with(scopes=["https://graph.microsoft.com/.default"])

    def test_get_access_token_raises_on_error_dict(self):
        self.mock_msal_app.acquire_token_for_client.return_value = {
            "error": "invalid_client",
            "error_description": "bad secret",
        }
        client = _make_client()
        with self.assertRaises(RuntimeError) as ctx:
            client._get_access_token()
        self.assertIn("bad secret", str(ctx.exception))


class TestGraphGet(unittest.TestCase):
    def setUp(self):
        self.msal_patcher = patch("utils.graph_client.msal.ConfidentialClientApplication")
        self.mock_msal_cls = self.msal_patcher.start()
        self.mock_msal_app = MagicMock()
        self.mock_msal_cls.return_value = self.mock_msal_app
        self.mock_msal_app.acquire_token_for_client.return_value = {"access_token": "tok-1"}

        self.session_patcher = patch("utils.graph_client.RetrySession")
        self.mock_session_cls = self.session_patcher.start()
        self.mock_api_session = MagicMock()
        self.mock_download_session = MagicMock()
        self.mock_session_cls.side_effect = [self.mock_api_session, self.mock_download_session]

    def tearDown(self):
        self.msal_patcher.stop()
        self.session_patcher.stop()

    def test_graph_get_success_returns_json(self):
        resp = MagicMock(ok=True, status_code=200)
        resp.json.return_value = {"value": [1, 2]}
        self.mock_api_session.get.return_value = resp

        client = _make_client()
        result = client._graph_get("/me/drive")

        self.assertEqual(result, {"value": [1, 2]})
        called_url = self.mock_api_session.get.call_args.args[0]
        self.assertEqual(called_url, "https://graph.microsoft.com/v1.0/me/drive")
        headers = self.mock_api_session.get.call_args.kwargs["headers"]
        self.assertEqual(headers["Authorization"], "Bearer tok-1")

    def test_graph_get_passes_through_full_urls_unchanged(self):
        resp = MagicMock(ok=True, status_code=200)
        resp.json.return_value = {}
        self.mock_api_session.get.return_value = resp

        client = _make_client()
        client._graph_get("https://graph.microsoft.com/v1.0/drives/x/items/y/children?$skiptoken=abc")

        called_url = self.mock_api_session.get.call_args.args[0]
        self.assertEqual(called_url, "https://graph.microsoft.com/v1.0/drives/x/items/y/children?$skiptoken=abc")

    def test_graph_get_raises_on_non_2xx(self):
        resp = MagicMock(ok=False, status_code=403)
        resp.json.return_value = {"error": {"message": "insufficient permissions"}}
        self.mock_api_session.get.return_value = resp

        client = _make_client()
        with self.assertRaises(RuntimeError) as ctx:
            client._graph_get("/drives/x/items/missing")
        self.assertIn("403", str(ctx.exception))
        self.assertNotIsInstance(ctx.exception, GraphItemNotFoundError)

    def test_graph_get_retries_once_on_401_then_succeeds(self):
        unauthorized = MagicMock(ok=False, status_code=401)
        unauthorized.json.return_value = {"error": {"message": "token expired"}}
        success = MagicMock(ok=True, status_code=200)
        success.json.return_value = {"id": "abc"}
        self.mock_api_session.get.side_effect = [unauthorized, success]

        client = _make_client()
        result = client._graph_get("/me/drive")

        self.assertEqual(result, {"id": "abc"})
        self.assertEqual(self.mock_api_session.get.call_count, 2)
        # Forced refresh rebuilds the MSAL app
        self.assertEqual(self.mock_msal_cls.call_count, 2)

    def test_graph_get_raises_after_second_401(self):
        unauthorized = MagicMock(ok=False, status_code=401)
        unauthorized.json.return_value = {"error": {"message": "still unauthorized"}}
        self.mock_api_session.get.side_effect = [unauthorized, unauthorized]

        client = _make_client()
        with self.assertRaises(RuntimeError):
            client._graph_get("/me/drive")
        self.assertEqual(self.mock_api_session.get.call_count, 2)

    def test_graph_get_raises_graph_item_not_found_on_404(self):
        """404 must raise a distinct type so callers (e.g. a folder walk) can choose to
        skip one missing item without treating it like a fatal auth/5xx failure."""
        not_found = MagicMock(ok=False, status_code=404)
        not_found.json.return_value = {"error": {"message": "item not found"}}
        self.mock_api_session.get.return_value = not_found

        client = _make_client()
        with self.assertRaises(GraphItemNotFoundError):
            client._graph_get("/drives/x/items/missing")

    def test_graph_item_not_found_is_a_runtime_error(self):
        not_found = MagicMock(ok=False, status_code=404)
        not_found.json.return_value = {"error": {"message": "item not found"}}
        self.mock_api_session.get.return_value = not_found

        client = _make_client()
        with self.assertRaises(RuntimeError):
            client._graph_get("/drives/x/items/missing")


class TestListChildren(unittest.TestCase):
    def setUp(self):
        self.msal_patcher = patch("utils.graph_client.msal.ConfidentialClientApplication")
        self.mock_msal_cls = self.msal_patcher.start()
        self.mock_msal_cls.return_value.acquire_token_for_client.return_value = {"access_token": "tok-1"}

        self.session_patcher = patch("utils.graph_client.RetrySession")
        self.mock_session_cls = self.session_patcher.start()
        self.mock_api_session = MagicMock()
        self.mock_download_session = MagicMock()
        self.mock_session_cls.side_effect = [self.mock_api_session, self.mock_download_session]

    def tearDown(self):
        self.msal_patcher.stop()
        self.session_patcher.stop()

    def _ok(self, body: dict) -> MagicMock:
        resp = MagicMock(ok=True, status_code=200)
        resp.json.return_value = body
        return resp

    def test_single_page(self):
        self.mock_api_session.get.return_value = self._ok({"value": [{"id": "a"}, {"id": "b"}]})

        client = _make_client()
        items = list(client.list_children("drive1", "item1"))

        self.assertEqual([i["id"] for i in items], ["a", "b"])
        self.assertEqual(self.mock_api_session.get.call_count, 1)
        called_url = self.mock_api_session.get.call_args.args[0]
        self.assertEqual(called_url, "https://graph.microsoft.com/v1.0/drives/drive1/items/item1/children")

    def test_follows_next_link_across_pages(self):
        page1 = self._ok(
            {"value": [{"id": "a"}], "@odata.nextLink": "https://graph.microsoft.com/v1.0/next?$skiptoken=xyz"}
        )
        page2 = self._ok({"value": [{"id": "b"}]})
        self.mock_api_session.get.side_effect = [page1, page2]

        client = _make_client()
        items = list(client.list_children("drive1", "item1"))

        self.assertEqual([i["id"] for i in items], ["a", "b"])
        self.assertEqual(self.mock_api_session.get.call_count, 2)
        second_call_url = self.mock_api_session.get.call_args_list[1].args[0]
        # nextLink is passed through unmodified — not reconstructed
        self.assertEqual(second_call_url, "https://graph.microsoft.com/v1.0/next?$skiptoken=xyz")

    def test_empty_folder_yields_nothing(self):
        self.mock_api_session.get.return_value = self._ok({"value": []})

        client = _make_client()
        items = list(client.list_children("drive1", "item1"))

        self.assertEqual(items, [])

    def test_special_characters_in_ids_are_percent_encoded(self):
        self.mock_api_session.get.return_value = self._ok({"value": []})

        client = _make_client()
        list(client.list_children("drive/1", "item#1"))

        called_url = self.mock_api_session.get.call_args.args[0]
        self.assertIn("drive%2F1", called_url)
        self.assertIn("item%231", called_url)


class TestGetItemByPath(unittest.TestCase):
    def setUp(self):
        self.msal_patcher = patch("utils.graph_client.msal.ConfidentialClientApplication")
        self.mock_msal_cls = self.msal_patcher.start()
        self.mock_msal_cls.return_value.acquire_token_for_client.return_value = {"access_token": "tok-1"}

        self.session_patcher = patch("utils.graph_client.RetrySession")
        self.mock_session_cls = self.session_patcher.start()
        self.mock_api_session = MagicMock()
        self.mock_download_session = MagicMock()
        self.mock_session_cls.side_effect = [self.mock_api_session, self.mock_download_session]

    def tearDown(self):
        self.msal_patcher.stop()
        self.session_patcher.stop()

    def test_encodes_each_segment_separately(self):
        resp = MagicMock(ok=True, status_code=200)
        resp.json.return_value = {"id": "abc"}
        self.mock_api_session.get.return_value = resp

        client = _make_client()
        client.get_item_by_path("drive1", "Documents/Q1 Report #2.pdf")

        called_url = self.mock_api_session.get.call_args.args[0]
        self.assertEqual(
            called_url,
            "https://graph.microsoft.com/v1.0/drives/drive1/root:/Documents/Q1%20Report%20%232.pdf",
        )

    def test_strips_leading_and_trailing_slashes(self):
        resp = MagicMock(ok=True, status_code=200)
        resp.json.return_value = {"id": "abc"}
        self.mock_api_session.get.return_value = resp

        client = _make_client()
        client.get_item_by_path("drive1", "/Documents/Reports/")

        called_url = self.mock_api_session.get.call_args.args[0]
        self.assertEqual(called_url, "https://graph.microsoft.com/v1.0/drives/drive1/root:/Documents/Reports")


class TestGetDownloadUrl(unittest.TestCase):
    def setUp(self):
        self.msal_patcher = patch("utils.graph_client.msal.ConfidentialClientApplication")
        self.mock_msal_cls = self.msal_patcher.start()
        self.mock_msal_cls.return_value.acquire_token_for_client.return_value = {"access_token": "tok-1"}

        self.session_patcher = patch("utils.graph_client.RetrySession")
        self.mock_session_cls = self.session_patcher.start()
        self.mock_api_session = MagicMock()
        self.mock_download_session = MagicMock()
        self.mock_session_cls.side_effect = [self.mock_api_session, self.mock_download_session]

    def tearDown(self):
        self.msal_patcher.stop()
        self.session_patcher.stop()

    def test_returns_fresh_download_url(self):
        resp = MagicMock(ok=True, status_code=200)
        resp.json.return_value = {"id": "item1", "@microsoft.graph.downloadUrl": "https://example.com/dl?tok=abc"}
        self.mock_api_session.get.return_value = resp

        client = _make_client()
        url = client.get_download_url("drive1", "item1")

        self.assertEqual(url, "https://example.com/dl?tok=abc")

    def test_raises_when_no_download_url_present(self):
        """Folders, OneNote packages, and similar items have no downloadUrl."""
        resp = MagicMock(ok=True, status_code=200)
        resp.json.return_value = {"id": "item1", "folder": {}}
        self.mock_api_session.get.return_value = resp

        client = _make_client()
        with self.assertRaises(RuntimeError):
            client.get_download_url("drive1", "item1")


class TestDownloadContent(unittest.TestCase):
    def setUp(self):
        self.msal_patcher = patch("utils.graph_client.msal.ConfidentialClientApplication")
        self.mock_msal_cls = self.msal_patcher.start()
        self.mock_msal_cls.return_value.acquire_token_for_client.return_value = {"access_token": "tok-1"}

        self.session_patcher = patch("utils.graph_client.RetrySession")
        self.mock_session_cls = self.session_patcher.start()
        self.mock_api_session = MagicMock()
        self.mock_download_session = MagicMock()
        self.mock_session_cls.side_effect = [self.mock_api_session, self.mock_download_session]

    def tearDown(self):
        self.msal_patcher.stop()
        self.session_patcher.stop()

    def _streamed_response(self, chunks: list[bytes], status_code: int = 200) -> MagicMock:
        resp = MagicMock(ok=status_code < 400, status_code=status_code)
        resp.iter_content.return_value = iter(chunks)
        return resp

    def test_downloads_and_joins_chunks(self):
        self.mock_download_session.get.return_value = self._streamed_response([b"hello ", b"world"])

        client = _make_client()
        content = client.download_content("https://example.com/dl?tok=abc")

        self.assertEqual(content, b"hello world")
        call_kwargs = self.mock_download_session.get.call_args
        self.assertEqual(call_kwargs.kwargs["stream"], True)

    def test_download_uses_download_session_not_api_session(self):
        self.mock_download_session.get.return_value = self._streamed_response([b"data"])

        client = _make_client()
        client.download_content("https://example.com/dl?tok=abc")

        self.mock_download_session.get.assert_called_once()
        self.mock_api_session.get.assert_not_called()

    def test_raises_on_non_ok_status(self):
        self.mock_download_session.get.return_value = self._streamed_response([], status_code=404)

        client = _make_client()
        with self.assertRaises(RuntimeError):
            client.download_content("https://example.com/dl?tok=abc")

    def test_raises_when_cumulative_size_exceeds_cap(self):
        # 3 chunks of 40 bytes each = 120 bytes, cap = 100
        chunks = [b"x" * 40, b"x" * 40, b"x" * 40]
        self.mock_download_session.get.return_value = self._streamed_response(chunks)

        client = _make_client(max_file_size_bytes=100)
        with self.assertRaises(RuntimeError) as ctx:
            client.download_content("https://example.com/dl?tok=abc")
        self.assertIn("max_file_size_bytes", str(ctx.exception))

    def test_response_closed_after_download(self):
        self.mock_download_session.get.return_value = self._streamed_response([b"data"])

        client = _make_client()
        client.download_content("https://example.com/dl?tok=abc")

        self.mock_download_session.get.return_value.close.assert_called_once()

    def test_response_closed_even_when_size_cap_exceeded(self):
        chunks = [b"x" * 200]
        self.mock_download_session.get.return_value = self._streamed_response(chunks)

        client = _make_client(max_file_size_bytes=100)
        with self.assertRaises(RuntimeError):
            client.download_content("https://example.com/dl?tok=abc")

        self.mock_download_session.get.return_value.close.assert_called_once()
