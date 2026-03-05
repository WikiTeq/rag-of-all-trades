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

    # --- source_type ---

    def test_source_type(self):
        job = SerpAPIIngestionJob(self.config)
        self.assertEqual(job.source_type, "serpapi")

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

    def test_queries_default_to_empty_when_missing(self):
        config = {"name": "serp1", "config": {"api_key": "k"}}
        job = SerpAPIIngestionJob(config)
        self.assertEqual(job.search_queries, [])

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

    def test_list_items_empty_when_no_queries(self):
        config = {"name": "serp1", "config": {"api_key": "k"}}
        job = SerpAPIIngestionJob(config)
        self.assertEqual(list(job.list_items()), [])

    # --- get_item_name ---

    def test_get_item_name_returns_query(self):
        job = SerpAPIIngestionJob(self.config)
        self.assertEqual(job.get_item_name("Python news"), "Python news")

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

        self.assertIn("Title 1", result)
        self.assertIn("Title 2", result)
        self.assertIn("Snippet 1", result)
        self.assertIn("Snippet 2", result)

    @patch("tasks.serpapi_ingestion.requests.get")
    def test_get_raw_content_skips_missing_titles_and_snippets(self, mock_get):
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

        self.assertIn("Title only", result)
        self.assertIn("Snippet only", result)

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
