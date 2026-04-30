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
    def test_post_method(self, MockSession):
        session = MockSession.return_value
        session.request.return_value = _make_response(201)

        rs = RetrySession()
        resp = rs.post("http://example.com", json={"key": "val"})

        self.assertEqual(resp.status_code, 201)
        call_kwargs = session.request.call_args
        self.assertEqual(call_kwargs[0][0], "POST")

    @patch("utils.http.requests.Session")
    def test_context_manager_closes_session(self, MockSession):
        session = MockSession.return_value
        session.request.return_value = _make_response(200)

        with RetrySession() as rs:
            rs.get("http://example.com")

        session.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
