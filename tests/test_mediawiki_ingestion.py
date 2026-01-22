import unittest
from unittest.mock import Mock, patch
import requests
from datetime import datetime
from tasks.mediawiki_ingestion import MediaWikiIngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem


class TestMediaWikiIngestionJob(unittest.TestCase):
    """Test MediaWiki ingestion job with mocked API responses."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            "name": "test_wiki",
            "config": {
                "api_url": "https://example.com/w/api.php"
            }
        }


    def _create_mock_session(self, mock_session_class):
        """Helper method to create a properly mocked session."""
        mock_session = Mock()
        mock_session_class.return_value = mock_session
        return mock_session

    def _create_mock_response(self, status_code=200, json_data=None, json_exception=None):
        """Helper method to create a properly mocked response."""
        mock_response = Mock()
        mock_response.status_code = status_code
        mock_response.raise_for_status = Mock()

        if json_exception:
            mock_response.json.side_effect = json_exception
        elif json_data is not None:
            mock_response.json.return_value = json_data

        return mock_response

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_page_info_success(self, mock_session_class):
        """Test successful page info retrieval with mocked API response."""
        mock_session = self._create_mock_session(mock_session_class)

        # Mock URL query response (first API call)
        mock_url_response = self._create_mock_response(json_data={
            "query": {
                "pages": {
                    "123": {
                        "pageid": 123,
                        "title": "Test Page",
                        "canonicalurl": "https://example.com/wiki/Test_Page"
                    }
                }
            }
        })

        # Mock parse response (second API call) - returns HTML
        mock_parse_response = self._create_mock_response(json_data={
            "parse": {
                "text": {
                    "*": "<p>Test page content with <a href='/wiki/Links'>links</a> and templates.</p>"
                }
            }
        })

        # Set up side_effect to return different responses for different calls
        mock_session.get.side_effect = [mock_url_response, mock_parse_response]

        # Create job and test
        job = MediaWikiIngestionJob(self.config)
        result = job._get_page_info("Test Page")

        # Verify results
        self.assertIsNotNone(result)
        content, url = result
        # Content is now cleaned HTML-to-text (Markdown format)
        self.assertIn("Test page content", content)
        self.assertEqual(url, "https://example.com/wiki/Test_Page")

        # Verify API calls were made correctly (2 calls: query for URL, parse for content)
        self.assertEqual(mock_session.get.call_count, 2)
        # First call should be query with info prop
        first_call = mock_session.get.call_args_list[0]
        self.assertEqual(first_call[1]['params']['action'], 'query')
        self.assertEqual(first_call[1]['params']['titles'], "Test Page")
        # Second call should be parse
        second_call = mock_session.get.call_args_list[1]
        self.assertEqual(second_call[1]['params']['action'], 'parse')
        self.assertEqual(second_call[1]['params']['page'], "Test Page")

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_page_info_missing_page(self, mock_session_class):
        """Test handling of missing pages."""
        mock_session = self._create_mock_session(mock_session_class)

        # Mock response for missing page
        mock_response = self._create_mock_response(json_data={
            "query": {
                "pages": {
                    "-1": {
                        "title": "Missing Page",
                        "missing": True
                    }
                }
            }
        })
        mock_session.get.return_value = mock_response

        job = MediaWikiIngestionJob(self.config)
        result = job._get_page_info("Missing Page")

        self.assertIsNone(result)

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_page_url_success(self, mock_session_class):
        """Test successful URL retrieval."""
        mock_session = self._create_mock_session(mock_session_class)

        # Mock URL query response (first API call)
        mock_url_response = self._create_mock_response(json_data={
            "query": {
                "pages": {
                    "456": {
                        "pageid": 456,
                        "title": "Another Page",
                        "canonicalurl": "https://example.com/wiki/Another_Page"
                    }
                }
            }
        })

        # Mock parse response (second API call) - _get_page_url calls _get_page_info which needs both
        mock_parse_response = self._create_mock_response(json_data={
            "parse": {
                "text": {
                    "*": "<p>Content</p>"
                }
            }
        })

        mock_session.get.side_effect = [mock_url_response, mock_parse_response]

        job = MediaWikiIngestionJob(self.config)
        result = job._get_page_url("Another Page")

        self.assertEqual(result, "https://example.com/wiki/Another_Page")

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_page_url_missing_url(self, mock_session_class):
        """Test handling when canonical URL is not provided."""
        mock_session = self._create_mock_session(mock_session_class)

        mock_response = self._create_mock_response(json_data={
            "query": {
                "pages": {
                    "789": {
                        "pageid": 789,
                        "title": "Page Without URL"
                        # No canonicalurl field
                    }
                }
            }
        })
        mock_session.get.return_value = mock_response

        job = MediaWikiIngestionJob(self.config)
        result = job._get_page_url("Page Without URL")

        self.assertIsNone(result)

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_source_type(self, mock_session_class):
        """Test source type property."""
        self._create_mock_session(mock_session_class)
        job = MediaWikiIngestionJob(self.config)
        self.assertEqual(job.source_type, "mediawiki")

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_initialization_validation(self, mock_session_class):
        """Test that initialization validates required config."""
        self._create_mock_session(mock_session_class)

        with self.assertRaises(ValueError):
            # Missing api_url
            MediaWikiIngestionJob({
                "name": "test",
                "config": {}
            })

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_initialization_numeric_validation(self, mock_session_class):
        """Test that initialization validates numeric config values."""
        self._create_mock_session(mock_session_class)

        # Test negative request_delay
        with self.assertRaises(ValueError):
            MediaWikiIngestionJob({
                "name": "test",
                "config": {"api_url": "https://example.com", "request_delay": -1}
            })

        # Test zero page_limit
        with self.assertRaises(ValueError):
            MediaWikiIngestionJob({
                "name": "test",
                "config": {"api_url": "https://example.com", "page_limit": 0}
            })

        # Test negative batch_size
        with self.assertRaises(ValueError):
            MediaWikiIngestionJob({
                "name": "test",
                "config": {"api_url": "https://example.com", "batch_size": -1}
            })

        # Test negative max_retries
        with self.assertRaises(ValueError):
            MediaWikiIngestionJob({
                "name": "test",
                "config": {"api_url": "https://example.com", "max_retries": -1}
            })

        # Test zero timeout
        with self.assertRaises(ValueError):
            MediaWikiIngestionJob({
                "name": "test",
                "config": {"api_url": "https://example.com", "timeout": 0}
            })

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_make_api_request_network_error(self, mock_session_class):
        """Test handling of network errors with retries and exponential backoff."""
        mock_session = self._create_mock_session(mock_session_class)

        # Mock network error
        mock_session.get.side_effect = requests.exceptions.RequestException("Network error")

        job = MediaWikiIngestionJob(self.config)
        result = job._make_api_request({'action': 'query'})

        # Should return None after all retries
        self.assertIsNone(result)

        # Should have made 3 attempts (max_retries)
        self.assertEqual(mock_session.get.call_count, 3)

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_make_api_request_rate_limiting(self, mock_session_class):
        """Test handling of rate limiting (429 responses) with Retry-After header."""
        mock_session = self._create_mock_session(mock_session_class)

        # Mock rate limited response
        mock_rate_limited_response = Mock()
        mock_rate_limited_response.status_code = 429
        mock_rate_limited_response.headers = {'Retry-After': '2'}

        # Mock successful response after rate limit
        mock_success_response = self._create_mock_response(json_data={"test": "data"})

        mock_session.get.side_effect = [mock_rate_limited_response, mock_success_response]

        job = MediaWikiIngestionJob(self.config)
        result = job._make_api_request({'action': 'query'})

        # Should eventually succeed after rate limit
        self.assertEqual(result, {"test": "data"})

        # Should have made 2 calls (rate limit + success)
        self.assertEqual(mock_session.get.call_count, 2)

    @patch('tasks.mediawiki_ingestion.requests.Session')
    @patch('tasks.mediawiki_ingestion.time.sleep')
    def test_make_api_request_timeout(self, mock_sleep, mock_session_class):
        """Test handling of timeout scenarios."""
        mock_session = self._create_mock_session(mock_session_class)

        # Mock timeout error
        mock_session.get.side_effect = requests.exceptions.Timeout("Request timed out")

        job = MediaWikiIngestionJob(self.config)
        result = job._make_api_request({'action': 'query'})

        # Should return None after retries
        self.assertIsNone(result)

        # Should have made 3 attempts
        self.assertEqual(mock_session.get.call_count, 3)

    @patch('tasks.mediawiki_ingestion.requests.Session')
    @patch('tasks.mediawiki_ingestion.time.sleep')
    def test_make_api_request_exponential_backoff(self, mock_sleep, mock_session_class):
        """Test exponential backoff behavior on failures."""
        mock_session = self._create_mock_session(mock_session_class)

        # Mock persistent network error
        mock_session.get.side_effect = requests.exceptions.RequestException("Network error")

        job = MediaWikiIngestionJob(self.config)
        result = job._make_api_request({'action': 'query'})

        # Should return None after all retries
        self.assertIsNone(result)

        # Verify exponential backoff delays: 2^0=1, 2^1=2 seconds (no sleep on final attempt)
        expected_delays = [1, 2]
        actual_delays = [call[0][0] for call in mock_sleep.call_args_list]
        self.assertEqual(actual_delays, expected_delays)

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_make_api_request_empty_response(self, mock_session_class):
        """Test handling of empty API responses."""
        mock_session = self._create_mock_session(mock_session_class)

        # Mock empty JSON response
        mock_response = self._create_mock_response(json_data={})
        mock_session.get.return_value = mock_response

        job = MediaWikiIngestionJob(self.config)
        result = job._make_api_request({'action': 'query'})

        # Should return empty dict
        self.assertEqual(result, {})

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_make_api_request_invalid_json(self, mock_session_class):
        """Test handling of invalid JSON responses."""
        mock_session = self._create_mock_session(mock_session_class)

        # Mock invalid JSON response
        mock_response = self._create_mock_response(json_exception=ValueError("Invalid JSON"))
        mock_session.get.return_value = mock_response

        job = MediaWikiIngestionJob(self.config)
        result = job._make_api_request({'action': 'query'})

        # Should return None due to JSON parsing error
        self.assertIsNone(result)

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_make_api_request_zero_retries(self, mock_session_class):
        """Test that zero max_retries returns None without making requests."""
        mock_session = self._create_mock_session(mock_session_class)

        job = MediaWikiIngestionJob(self.config)
        result = job._make_api_request({'action': 'query'}, max_retries=0)

        # Should return None immediately without making any requests
        self.assertIsNone(result)
        mock_session.get.assert_not_called()

    @patch('tasks.mediawiki_ingestion.requests.Session')
    @patch('tasks.mediawiki_ingestion.time.sleep')
    def test_list_items_success(self, mock_sleep, mock_session_class):
        """Test successful listing of pages with batched timestamp fetching."""
        mock_session = self._create_mock_session(mock_session_class)

        # Mock _get_all_pages response (single page with no continuation)
        mock_allpages_response = self._create_mock_response(json_data={
            "query": {
                "allpages": [
                    {"title": "Test Page 1"},
                    {"title": "Test Page 2"}
                ]
            }
        })

        # Mock batched timestamp response for both pages
        mock_timestamp_response = self._create_mock_response(json_data={
            "query": {
                "pages": {
                    "123": {
                        "title": "Test Page 1",
                        "pageid": 123,
                        "revisions": [{"timestamp": "2024-01-01T12:00:00Z"}]
                    },
                    "124": {
                        "title": "Test Page 2",
                        "pageid": 124,
                        "revisions": [{"timestamp": "2024-01-02T12:00:00Z"}]
                    }
                }
            }
        })

        mock_session.get.side_effect = [mock_allpages_response, mock_timestamp_response]

        job = MediaWikiIngestionJob(self.config)
        items = list(job.list_items())

        # Should return 2 items
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].id, "mediawiki:Test Page 1")
        self.assertEqual(items[1].id, "mediawiki:Test Page 2")

        # Should have made only 2 API calls: 1 allpages + 1 batched timestamps
        self.assertEqual(mock_session.get.call_count, 2)

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_raw_content_success(self, mock_session_class):
        """Test successful raw content retrieval."""
        mock_session = self._create_mock_session(mock_session_class)

        # Mock URL query response (first API call)
        mock_url_response = self._create_mock_response(json_data={
            "query": {
                "pages": {
                    "123": {
                        "pageid": 123,
                        "title": "Test Page",
                        "canonicalurl": "https://example.com/wiki/Test_Page"
                    }
                }
            }
        })

        # Mock parse response (second API call) - returns HTML
        mock_parse_response = self._create_mock_response(json_data={
            "parse": {
                "text": {
                    "*": "<p>Test content</p>"
                }
            }
        })

        mock_session.get.side_effect = [mock_url_response, mock_parse_response]

        job = MediaWikiIngestionJob(self.config)
        item = IngestionItem(id="mediawiki:Test Page", source_ref="Test Page")
        content = job.get_raw_content(item)

        # Content is now cleaned HTML-to-text (Markdown format)
        self.assertIn("Test content", content)
        # URL should be cached in metadata cache
        self.assertEqual(item._metadata_cache.get('page_url'), "https://example.com/wiki/Test_Page")

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_raw_content_missing_page(self, mock_session_class):
        """Test raw content retrieval for missing page."""
        mock_session = self._create_mock_session(mock_session_class)

        mock_response = self._create_mock_response(json_data={
            "query": {
                "pages": {
                    "-1": {
                        "title": "Missing Page",
                        "missing": True
                    }
                }
            }
        })
        mock_session.get.return_value = mock_response

        job = MediaWikiIngestionJob(self.config)
        item = IngestionItem(id="mediawiki:Missing Page", source_ref="Missing Page")
        content = job.get_raw_content(item)

        self.assertEqual(content, "")
        # URL should not be cached for missing pages
        self.assertNotIn('page_url', item._metadata_cache)

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_item_name_basic(self, mock_session_class):
        """Test basic item name generation."""
        self._create_mock_session(mock_session_class)
        job = MediaWikiIngestionJob(self.config)

        item = IngestionItem(id="mediawiki:Test Page", source_ref="Test Page")
        name = job.get_item_name(item)

        self.assertEqual(name, "Test_Page")

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_item_name_edge_cases(self, mock_session_class):
        """Test item name generation with edge cases."""
        self._create_mock_session(mock_session_class)
        config = self.config  # Use the test config

        # Test with special characters
        item1 = IngestionItem(id="mediawiki:Page/With:Special*Chars?", source_ref="Page/With:Special*Chars?")
        result1 = MediaWikiIngestionJob(config).get_item_name(item1)
        self.assertEqual(result1, "Page_With_Special_Chars")

        # Test with very long title (>255 chars)
        long_title = "A" * 300
        item2 = IngestionItem(id=f"mediawiki:{long_title}", source_ref=long_title)
        result2 = MediaWikiIngestionJob(config).get_item_name(item2)
        self.assertEqual(len(result2), 255)
        self.assertTrue(result2.endswith("A"))

        # Test with unicode characters
        unicode_title = "Página_tëst_中文_🚀"
        item3 = IngestionItem(id=f"mediawiki:{unicode_title}", source_ref=unicode_title)
        result3 = MediaWikiIngestionJob(config).get_item_name(item3)
        self.assertEqual(result3, "Página_tëst_中文")

        # Test with leading/trailing underscores
        item4 = IngestionItem(id="mediawiki:_Test_Page_", source_ref="_Test_Page_")
        result4 = MediaWikiIngestionJob(config).get_item_name(item4)
        self.assertEqual(result4, "Test_Page")


    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_all_pages_single_page(self, mock_session_class):
        """Test _get_all_pages with single page response (no continuation)."""
        mock_session = self._create_mock_session(mock_session_class)

        mock_response = self._create_mock_response(json_data={
            "query": {
                "allpages": [
                    {"title": "Page 1"},
                    {"title": "Page 2"}
                ]
            }
        })
        mock_session.get.return_value = mock_response

        job = MediaWikiIngestionJob(self.config)
        pages = list(job._get_all_pages())

        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0]["title"], "Page 1")
        self.assertEqual(pages[1]["title"], "Page 2")

    @patch('tasks.mediawiki_ingestion.requests.Session')
    @patch('tasks.mediawiki_ingestion.time.sleep')
    def test_get_all_pages_pagination(self, mock_sleep, mock_session_class):
        """Test _get_all_pages with pagination (continuation tokens)."""
        mock_session = self._create_mock_session(mock_session_class)

        # First response with continuation
        first_response = self._create_mock_response(json_data={
            "query": {
                "allpages": [
                    {"title": "Page 1"},
                    {"title": "Page 2"}
                ]
            },
            "continue": {
                "apcontinue": "Page_3",
                "continue": "-||"
            }
        })

        # Second response (no continuation)
        second_response = self._create_mock_response(json_data={
            "query": {
                "allpages": [
                    {"title": "Page 3"},
                    {"title": "Page 4"}
                ]
            }
        })

        mock_session.get.side_effect = [first_response, second_response]

        job = MediaWikiIngestionJob(self.config)
        pages = list(job._get_all_pages())

        self.assertEqual(len(pages), 4)
        self.assertEqual(pages[0]["title"], "Page 1")
        self.assertEqual(pages[2]["title"], "Page 3")

        # Should have made 2 requests
        self.assertEqual(mock_session.get.call_count, 2)

        # Should have called sleep once between requests
        mock_sleep.assert_called_once()

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_all_pages_namespace_filtering(self, mock_session_class):
        """Test _get_all_pages with namespace filtering."""
        mock_session = self._create_mock_session(mock_session_class)

        # Create config with namespace filtering
        config_with_namespaces = {
            "name": "test_wiki",
            "config": {
                "api_url": "https://example.com/w/api.php",
                "namespaces": [0, 1, 2]  # Main, Talk, User namespaces
            }
        }

        mock_response = self._create_mock_response(json_data={
            "query": {
                "allpages": [
                    {"title": "Main Page"},
                    {"title": "Talk Page"}
                ]
            }
        })
        mock_session.get.return_value = mock_response

        job = MediaWikiIngestionJob(config_with_namespaces)
        pages = list(job._get_all_pages())

        self.assertEqual(len(pages), 2)

        # Verify the API call included multiple apnamespace parameters
        call_args = mock_session.get.call_args
        params = call_args[1]['params']
        self.assertIn('apnamespace', params)
        # Should be a list of strings for multiple namespace parameters
        self.assertEqual(params['apnamespace'], ['0', '1', '2'])

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_page_last_modified_success(self, mock_session_class):
        """Test successful last modified timestamp retrieval."""
        mock_session = self._create_mock_session(mock_session_class)

        mock_response = self._create_mock_response(json_data={
            "query": {
                "pages": {
                    "123": {
                        "title": "Test Page",
                        "pageid": 123,
                        "revisions": [{
                            "timestamp": "2024-01-01T12:00:00Z"
                        }]
                    }
                }
            }
        })
        mock_session.get.return_value = mock_response

        job = MediaWikiIngestionJob(self.config)
        timestamps = job._get_pages_last_modified(["Test Page"])
        timestamp = timestamps["Test Page"]

        self.assertIsNotNone(timestamp)
        self.assertEqual(timestamp.year, 2024)
        self.assertEqual(timestamp.month, 1)
        self.assertEqual(timestamp.day, 1)

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_page_last_modified_missing_page(self, mock_session_class):
        """Test last modified for missing page."""
        mock_session = self._create_mock_session(mock_session_class)

        mock_response = self._create_mock_response(json_data={
            "query": {
                "pages": {
                    "-1": {
                        "title": "Missing Page",
                        "missing": True
                    }
                }
            }
        })
        mock_session.get.return_value = mock_response

        job = MediaWikiIngestionJob(self.config)
        timestamps = job._get_pages_last_modified(["Missing Page"])
        timestamp = timestamps["Missing Page"]

        self.assertIsNone(timestamp)

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_page_last_modified_no_revisions(self, mock_session_class):
        """Test last modified when page has no revisions."""
        mock_session = self._create_mock_session(mock_session_class)

        mock_response = self._create_mock_response(json_data={
            "query": {
                "pages": {
                    "123": {
                        "pageid": 123,
                        "title": "Test Page"
                        # No revisions field
                    }
                }
            }
        })
        mock_session.get.return_value = mock_response

        job = MediaWikiIngestionJob(self.config)
        timestamps = job._get_pages_last_modified(["Test Page"])
        timestamp = timestamps["Test Page"]

        self.assertIsNone(timestamp)

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_page_last_modified_with_timezone_offset(self, mock_session_class):
        """Test last modified with timezone offset timestamps."""
        mock_session = self._create_mock_session(mock_session_class)

        mock_response = self._create_mock_response(json_data={
            "query": {
                "pages": {
                    "123": {
                        "title": "Test Page",
                        "pageid": 123,
                        "revisions": [{
                            "timestamp": "2024-01-01T12:00:00-05:00"
                        }]
                    }
                }
            }
        })
        mock_session.get.return_value = mock_response

        job = MediaWikiIngestionJob(self.config)
        timestamps = job._get_pages_last_modified(["Test Page"])
        timestamp = timestamps["Test Page"]

        self.assertIsNotNone(timestamp)
        # Should parse -05:00 timezone correctly
        self.assertEqual(timestamp.year, 2024)
        self.assertEqual(timestamp.month, 1)
        self.assertEqual(timestamp.day, 1)
        self.assertEqual(timestamp.hour, 12)  # Hour in original timezone (-05:00)
        # The datetime preserves timezone info, so hour remains 12 in local time
        # (which corresponds to 17:00 UTC)

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_page_last_modified_invalid_timestamp(self, mock_session_class):
        """Test last modified with invalid timestamp format."""
        mock_session = self._create_mock_session(mock_session_class)

        mock_response = self._create_mock_response(json_data={
            "query": {
                "pages": {
                    "123": {
                        "title": "Test Page",
                        "pageid": 123,
                        "revisions": [{
                            "timestamp": "invalid-timestamp"
                        }]
                    }
                }
            }
        })
        mock_session.get.return_value = mock_response

        job = MediaWikiIngestionJob(self.config)
        timestamps = job._get_pages_last_modified(["Test Page"])
        timestamp = timestamps["Test Page"]

        self.assertIsNone(timestamp)

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_pages_last_modified_empty_list(self, mock_session_class):
        """Test _get_pages_last_modified with empty list."""
        mock_session = self._create_mock_session(mock_session_class)

        job = MediaWikiIngestionJob(self.config)
        timestamps = job._get_pages_last_modified([])

        self.assertEqual(timestamps, {})

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_document_metadata_with_cached_url(self, mock_session_class):
        """Test document metadata generation with cached URL."""
        mock_session = self._create_mock_session(mock_session_class)

        job = MediaWikiIngestionJob(self.config)

        item = IngestionItem(
            id="mediawiki:Test Page",
            source_ref="Test Page",
            last_modified=datetime(2024, 1, 1, 12, 0, 0)
        )
        # Set cached URL in metadata cache (simulating what process_item does)
        object.__setattr__(item, '_metadata_cache', {'page_url': "https://example.com/wiki/Test_Page"})

        metadata = job.get_document_metadata(
            item=item,
            item_name="Test_Page.md",
            checksum="abc123",
            version=1,
            last_modified=datetime(2024, 1, 1, 12, 0, 0)
        )

        # Check base metadata fields
        self.assertEqual(metadata["source"], "mediawiki")
        self.assertEqual(metadata["key"], "Test_Page.md")
        self.assertEqual(metadata["checksum"], "abc123")
        self.assertEqual(metadata["version"], 1)
        self.assertEqual(metadata["format"], "markdown")
        self.assertEqual(metadata["source_name"], "test_wiki")
        self.assertEqual(metadata["file_name"], "Test_Page.md")
        self.assertEqual(metadata["last_modified"], "2024-01-01 12:00:00")

        # Check MediaWiki-specific URL field
        self.assertEqual(metadata["url"], "https://example.com/wiki/Test_Page")

        # Should not make API calls since URL was cached
        mock_session.get.assert_not_called()

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_document_metadata_with_fetched_url(self, mock_session_class):
        """Test document metadata generation with URL cached from process_item."""
        mock_session = self._create_mock_session(mock_session_class)

        job = MediaWikiIngestionJob(self.config)

        item = IngestionItem(
            id="mediawiki:Test Page",
            source_ref="Test Page",
            last_modified=datetime(2024, 1, 1, 12, 0, 0)
        )
        # Cache URL in metadata cache (as process_item does)
        object.__setattr__(item, '_metadata_cache', {'page_url': "https://example.com/wiki/Test_Page"})

        metadata = job.get_document_metadata(
            item=item,
            item_name="Test_Page.md",
            checksum="abc123",
            version=1,
            last_modified=datetime(2024, 1, 1, 12, 0, 0)
        )

        # Check base metadata fields
        self.assertEqual(metadata["source"], "mediawiki")
        self.assertEqual(metadata["key"], "Test_Page.md")
        self.assertEqual(metadata["checksum"], "abc123")
        self.assertEqual(metadata["version"], 1)

        # Check MediaWiki-specific URL field from cache
        self.assertEqual(metadata["url"], "https://example.com/wiki/Test_Page")

        # Should not make API calls since URL was cached
        mock_session.get.assert_not_called()

    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_get_document_metadata_url_fetch_fails(self, mock_session_class):
        """Test document metadata generation when URL is not cached."""
        mock_session = self._create_mock_session(mock_session_class)

        job = MediaWikiIngestionJob(self.config)

        item = IngestionItem(
            id="mediawiki:Test Page",
            source_ref="Test Page",
            last_modified=datetime(2024, 1, 1, 12, 0, 0)
        )
        # No cached URL in metadata cache (simulating failure case)

        metadata = job.get_document_metadata(
            item=item,
            item_name="Test_Page.md",
            checksum="abc123",
            version=1,
            last_modified=datetime(2024, 1, 1, 12, 0, 0)
        )

        # Check base metadata fields are still present
        self.assertEqual(metadata["source"], "mediawiki")
        self.assertEqual(metadata["key"], "Test_Page.md")
        self.assertEqual(metadata["checksum"], "abc123")

        # URL field should not be present since it wasn't cached
        self.assertNotIn("url", metadata)

        # Should not make API calls - URL should be cached by process_item before this is called
        mock_session.get.assert_not_called()

    @patch('tasks.mediawiki_ingestion.time.sleep')
    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_process_item_success(self, mock_session_class, mock_sleep):
        """Test successful processing of an ingestion item."""
        mock_session = self._create_mock_session(mock_session_class)

        # Mock URL query response (first API call)
        mock_url_response = self._create_mock_response(json_data={
            "query": {
                "pages": {
                    "123": {
                        "pageid": 123,
                        "title": "Test Page",
                        "canonicalurl": "https://example.com/wiki/Test_Page"
                    }
                }
            }
        })

        # Mock parse response (second API call)
        mock_parsed_response = self._create_mock_response(json_data={
            "parse": {
                "text": {"*": "<p>This is parsed content.</p>"}
            }
        })

        mock_session.get.side_effect = [mock_url_response, mock_parsed_response]

        # Mock metadata tracker and vector manager
        job = MediaWikiIngestionJob(self.config)
        with patch.object(job.metadata_tracker, 'get_latest_record', return_value=None):
            with patch.object(job.metadata_tracker, 'record_metadata'):
                with patch.object(job.metadata_tracker, 'delete_previous_embeddings'):
                    job.vector_manager.insert_documents = Mock()

                    # Create test item
                    from datetime import datetime
                    item = IngestionItem(
                        id="mediawiki:Test Page",
                        source_ref="Test Page",
                        last_modified=datetime(2024, 1, 1, 12, 0, 0)
                    )

                    # Process the item
                    result = job.process_item(item)

                    # Verify success
                    self.assertEqual(result, 1)

                    # Verify metadata tracking was called
                    job.metadata_tracker.record_metadata.assert_called_once()

                    # Verify vector store was called
                    job.vector_manager.insert_documents.assert_called_once()

    @patch('tasks.mediawiki_ingestion.time.sleep')
    @patch('tasks.mediawiki_ingestion.requests.Session')
    def test_process_item_duplicate_content(self, mock_session_class, mock_sleep):
        """Test processing of duplicate content (should skip)."""
        mock_session = self._create_mock_session(mock_session_class)

        # Mock URL query response (first API call)
        mock_url_response = self._create_mock_response(json_data={
            "query": {
                "pages": {
                    "123": {
                        "pageid": 123,
                        "title": "Test Page",
                        "canonicalurl": "https://example.com/wiki/Test_Page"
                    }
                }
            }
        })

        # Mock parse response (second API call)
        mock_parsed_response = self._create_mock_response(json_data={
            "parse": {
                "text": {"*": "<p>Duplicate content.</p>"}
            }
        })

        mock_session.get.side_effect = [mock_url_response, mock_parsed_response]

        # Mock metadata tracker - simulate duplicate detection
        job = MediaWikiIngestionJob(self.config)
        with patch.object(job.metadata_tracker, 'get_latest_record', return_value=None):
            with patch.object(job.metadata_tracker, 'record_metadata'):
                with patch.object(job.metadata_tracker, 'delete_previous_embeddings'):
                    job.vector_manager.insert_documents = Mock()

                    # Mock that content was already seen (duplicate)
                    # _seen_add returns False for duplicates
                    original_seen_add = job._seen_add
                    job._seen_add = Mock(return_value=False)  # False means duplicate

                    # Create test item
                    from datetime import datetime
                    item = IngestionItem(
                        id="mediawiki:Test Page",
                        source_ref="Test Page",
                        last_modified=datetime(2024, 1, 1, 12, 0, 0)
                    )

                    # Process the item
                    result = job.process_item(item)

                    # Verify it was skipped due to duplicate
                    self.assertEqual(result, 0)

                    # Verify metadata tracking was NOT called
                    job.metadata_tracker.record_metadata.assert_not_called()

                    # Verify vector store was NOT called
                    job.vector_manager.insert_documents.assert_not_called()

    def test_html_to_clean_text_improved_formatting(self):
        """Test that HTML cleaning converts to Markdown while preserving structure."""
        job = MediaWikiIngestionJob(self.config)

        # Test case: bold text converts to Markdown **bold** (html2text behavior)
        html_with_bold = "<p>This firm enjoys <b>mangoes</b>. It's our favorite fruit!</p>"
        result = job._html_to_clean_text(html_with_bold)
        # html2text converts <b> to **bold** in Markdown
        self.assertIn("mangoes", result)
        self.assertIn("This firm enjoys", result)
        # Should not have line breaks within the sentence
        self.assertNotIn("mangoes\n", result)

        # Test case: multiple inline elements convert to Markdown
        html_complex = "<p>We have <i>delicious</i> <b>mangoes</b> and <strong>bananas</strong>.</p>"
        result = job._html_to_clean_text(html_complex)
        self.assertIn("mangoes", result)
        self.assertIn("bananas", result)
        # Should not break flow with newlines
        self.assertNotIn("mangoes\n", result)

        # Test case: links should not break flow (html2text ignores links by config)
        html_with_links = "<p>Check out <a href='#'>our website</a> for more info.</p>"
        result = job._html_to_clean_text(html_with_links)
        self.assertIn("our website", result)
        self.assertIn("Check out", result)

        # Test case: block elements should create proper breaks
        html_with_paragraphs = "<p>First paragraph.</p><p>Second paragraph.</p>"
        result = job._html_to_clean_text(html_with_paragraphs)
        self.assertIn("First paragraph", result)
        self.assertIn("Second paragraph", result)
        # Should have separation between paragraphs
        self.assertGreater(len(result.split("\n")), 1)

        # Test case: mixed content
        html_mixed = "<h1>Title</h1><p>This is <b>bold</b> and <i>italic</i> text.</p><p>Another paragraph.</p>"
        result = job._html_to_clean_text(html_mixed)
        # Should have title, then paragraphs with proper spacing
        self.assertIn("Title", result)
        self.assertIn("bold", result)
        self.assertIn("italic", result)
        self.assertIn("Another paragraph", result)
        # Ensure no excessive line breaks within sentences
        self.assertNotIn("bold\n", result)
        self.assertNotIn("italic\n", result)


if __name__ == '__main__':
    unittest.main()
