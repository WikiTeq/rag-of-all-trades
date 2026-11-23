import unittest
from unittest.mock import MagicMock, patch

import requests

from utils.http import RetrySession


def _make_response(status_code: int, headers: dict | None = None) -> MagicMock:
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.headers = headers or {}
    return resp


class TestRetrySession(unittest.TestCase):
    def setUp(self):
        self.patcher = patch("utils.http.time.sleep")
        self.mock_sleep = self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    @patch("utils.http.requests.Session")
    def test_success_on_first_attempt(self, MockSession):
        session = MockSession.return_value
        session.request.return_value = _make_response(200)

        rs = RetrySession()
        resp = rs.get("http://example.com")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(session.request.call_count, 1)
        self.mock_sleep.assert_not_called()

    @patch("utils.http.requests.Session")
    def test_retries_on_429_with_retry_after(self, MockSession):
        session = MockSession.return_value
        session.request.side_effect = [
            _make_response(429, {"Retry-After": "5"}),
            _make_response(200),
        ]

        rs = RetrySession(max_retries=3)
        resp = rs.get("http://example.com")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(session.request.call_count, 2)
        self.mock_sleep.assert_called_once_with(5)

    @patch("utils.http.requests.Session")
    def test_retries_on_500(self, MockSession):
        session = MockSession.return_value
        session.request.side_effect = [
            _make_response(500),
            _make_response(200),
        ]

        rs = RetrySession(max_retries=3)
        resp = rs.get("http://example.com")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(session.request.call_count, 2)

    @patch("utils.http.requests.Session")
    def test_retries_on_network_error(self, MockSession):
        session = MockSession.return_value
        session.request.side_effect = [
            requests.ConnectionError("boom"),
            _make_response(200),
        ]

        rs = RetrySession(max_retries=3)
        resp = rs.get("http://example.com")

        self.assertEqual(resp.status_code, 200)

    @patch("utils.http.requests.Session")
    def test_retry_log_does_not_contain_urls_from_the_exception(self, MockSession):
        session = MockSession.return_value
        session.request.side_effect = [
            requests.ConnectionError("HTTPSConnectionPool: Max retries with url: /f?tempauth=SECRET (host='x')"),
            requests.ConnectionError("Failed for https://download.example/f?tempauth=SECRET"),
            _make_response(200),
        ]

        with self.assertLogs("utils.http", level="WARNING") as logs:
            RetrySession().get("http://example.com")

        self.assertIn("<url>", "".join(logs.output))
        self.assertNotIn("SECRET", "".join(logs.output))
        self.assertNotIn("download.example", "".join(logs.output))

    @patch("utils.http.requests.Session")
    def test_retry_log_redacts_paths_with_parentheses_and_apostrophes(self, MockSession):
        for path in ("/clip(1).mp4", "/Docs/O'Brien.mp4", '/say"hi".mp4'):
            with self.subTest(path=path):
                session = MockSession.return_value
                session.request.side_effect = [
                    requests.ConnectionError(f"Max retries exceeded with url: {path}?tempauth=SECRET (Caused by X)"),
                    requests.ConnectionError(f"Failed for https://download.example{path}?tempauth=SECRET"),
                    _make_response(200),
                ]

                with self.assertLogs("utils.http", level="WARNING") as logs:
                    RetrySession().get("http://example.com")

                self.assertNotIn("SECRET", "".join(logs.output))
                self.assertNotIn("Brien", "".join(logs.output))
                self.assertNotIn("clip(1)", "".join(logs.output))

    @patch("utils.http.requests.Session")
    def test_raises_after_max_retries_exceeded(self, MockSession):
        session = MockSession.return_value
        session.request.side_effect = requests.ConnectionError("always fails")

        rs = RetrySession(max_retries=2)
        with self.assertRaises(requests.ConnectionError):
            rs.get("http://example.com")

        self.assertEqual(session.request.call_count, 3)
        # Must not sleep after the final attempt
        self.assertEqual(self.mock_sleep.call_count, 2)

    @patch("utils.http.requests.Session")
    def test_no_sleep_after_final_429(self, MockSession):
        session = MockSession.return_value
        session.request.return_value = _make_response(429, {"Retry-After": "5"})

        rs = RetrySession(max_retries=1)
        rs.get("http://example.com")

        # Only 1 sleep: after attempt 0; attempt 1 is last so no sleep
        self.assertEqual(self.mock_sleep.call_count, 1)

    @patch("utils.http.requests.Session")
    def test_get_stream_defaults_to_false(self, MockSession):
        session = MockSession.return_value
        session.request.return_value = _make_response(200)

        rs = RetrySession()
        rs.get("http://example.com")

        self.assertEqual(session.request.call_args.kwargs["stream"], False)

    @patch("utils.http.requests.Session")
    def test_get_stream_true_is_forwarded(self, MockSession):
        session = MockSession.return_value
        session.request.return_value = _make_response(200)

        rs = RetrySession()
        rs.get("http://example.com", stream=True)

        self.assertEqual(session.request.call_args.kwargs["stream"], True)

    @patch("utils.http.requests.Session")
    def test_post_method(self, MockSession):
        session = MockSession.return_value
        session.request.return_value = _make_response(201)

        rs = RetrySession()
        resp = rs.post("http://example.com", json={"key": "val"})

        self.assertEqual(resp.status_code, 201)
        call_kwargs = session.request.call_args
        self.assertEqual(call_kwargs[0][0], "POST")

    @patch("utils.http.requests.Session")
    def test_post_method_with_form_encoded_data(self, MockSession):
        session = MockSession.return_value
        session.request.return_value = _make_response(200)

        rs = RetrySession()
        resp = rs.post("http://example.com", data={"grant_type": "client_credentials"})

        self.assertEqual(resp.status_code, 200)
        call_kwargs = session.request.call_args
        self.assertEqual(call_kwargs[0][0], "POST")
        self.assertEqual(call_kwargs.kwargs["data"], {"grant_type": "client_credentials"})
        self.assertIsNone(call_kwargs.kwargs["json"])

    @patch("utils.http.requests.Session")
    def test_context_manager_closes_session(self, MockSession):
        session = MockSession.return_value
        session.request.return_value = _make_response(200)

        with RetrySession() as rs:
            rs.get("http://example.com")

        session.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
