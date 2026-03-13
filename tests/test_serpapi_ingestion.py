import unittest
from unittest.mock import Mock, patch

import requests

from tasks.serpapi_ingestion import SerpAPIIngestionJob


class TestSerpAPIIngestionJob(unittest.TestCase):
    def setUp(self):
        self.config = {
            "name": "serp1",
            "config": {
                "api_key": "test-key",
                "queries": "Python news, AI trends",
            },
        }

    def _make_response(
        self, status_code=200, json_data=None, raise_for_status=None
    ):
        mock_resp = Mock()
        mock_resp.status_code = status_code
        if raise_for_status:
            mock_resp.raise_for_status.side_effect = raise_for_status
        else:
            mock_resp.raise_for_status = Mock()
        mock_resp.json.return_value = json_data or {}
        return mock_resp

    # --- __init__ / query parsing ---

    def test_queries_parsed_from_comma_string(self):
        job = SerpAPIIngestionJob(self.config)
        self.assertEqual(job.search_queries, ["Python news", "AI trends"])

    def test_queries_parsed_from_list(self):
        config = {
            "name": "serp1",
            "config": {"api_key": "k", "queries": ["q1", "q2"]},
        }
        job = SerpAPIIngestionJob(config)
        self.assertEqual(job.search_queries, ["q1", "q2"])

    def test_raises_when_queries_missing(self):
        config = {"name": "serp1", "config": {"api_key": "k"}}
        with self.assertRaises(ValueError):
            SerpAPIIngestionJob(config)

    def test_queries_strips_whitespace(self):
        config = {
            "name": "serp1",
            "config": {"api_key": "k", "queries": " q1 ,  q2 "},
        }
        job = SerpAPIIngestionJob(config)
        self.assertEqual(job.search_queries, ["q1", "q2"])

    # --- list_items ---

    def test_list_items_returns_queries(self):
        job = SerpAPIIngestionJob(self.config)
        self.assertEqual(list(job.list_items()), ["Python news", "AI trends"])

    def test_raises_when_queries_empty_string(self):
        config = {"name": "serp1", "config": {"api_key": "k", "queries": "  ,  "}}
        with self.assertRaises(ValueError):
            SerpAPIIngestionJob(config)

    # --- get_raw_content ---

    @patch("tasks.serpapi_ingestion.requests.get")
    def test_get_raw_content_returns_titles_and_snippets(self, mock_get):
        mock_get.return_value = self._make_response(
            json_data={
                "organic_results": [
                    {"title": "Title 1", "snippet": "Snippet 1"},
                    {"title": "Title 2", "snippet": "Snippet 2"},
                ]
            }
        )

        job = SerpAPIIngestionJob(self.config)
        result = job.get_raw_content("Python news")

        self.assertEqual(result, "Title 1\nTitle 2\nSnippet 1\nSnippet 2")

    @patch("tasks.serpapi_ingestion.requests.get")
    def test_get_raw_content_includes_partial_results(self, mock_get):
        mock_get.return_value = self._make_response(
            json_data={
                "organic_results": [
                    {"title": "Title only"},
                    {"snippet": "Snippet only"},
                    {},
                ]
            }
        )

        job = SerpAPIIngestionJob(self.config)
        result = job.get_raw_content("Python news")

        self.assertEqual(result, "Title only\nSnippet only")

    @patch("tasks.serpapi_ingestion.requests.get")
    def test_get_raw_content_returns_empty_on_no_organic_results(
        self, mock_get
    ):
        mock_get.return_value = self._make_response(
            json_data={"organic_results": []}
        )

        job = SerpAPIIngestionJob(self.config)
        result = job.get_raw_content("Python news")

        self.assertEqual(result, "")

    @patch("tasks.serpapi_ingestion.requests.get")
    def test_get_raw_content_returns_empty_on_http_error(self, mock_get):
        mock_get.return_value = self._make_response(
            status_code=403,
            raise_for_status=requests.exceptions.HTTPError("403 Forbidden"),
        )

        job = SerpAPIIngestionJob(self.config)
        result = job.get_raw_content("Python news")

        self.assertEqual(result, "")

    @patch("tasks.serpapi_ingestion.requests.get")
    def test_get_raw_content_returns_empty_on_network_error(self, mock_get):
        mock_get.side_effect = requests.exceptions.ConnectionError(
            "no route to host"
        )

        job = SerpAPIIngestionJob(self.config)
        result = job.get_raw_content("Python news")

        self.assertEqual(result, "")

    @patch("tasks.serpapi_ingestion.requests.get")
    def test_get_raw_content_passes_correct_params(self, mock_get):
        mock_get.return_value = self._make_response(
            json_data={"organic_results": []}
        )

        job = SerpAPIIngestionJob(self.config)
        job.get_raw_content("Python news")

        mock_get.assert_called_once_with(
            "https://serpapi.com/search",
            params={
                "engine": "google",
                "q": "Python news",
                "api_key": "test-key",
            },
        )

    @patch("tasks.serpapi_ingestion.requests.get")
    def test_get_raw_content_logs_warning_on_error(self, mock_get):
        mock_get.side_effect = requests.exceptions.ConnectionError("fail")

        job = SerpAPIIngestionJob(self.config)
        with patch("tasks.serpapi_ingestion.logger") as mock_logger:
            job.get_raw_content("Python news")
            mock_logger.info.assert_called_once()
            self.assertIn("Python news", mock_logger.info.call_args[0][0])


if __name__ == "__main__":
    unittest.main()
