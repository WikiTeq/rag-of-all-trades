"""Tests for MediaWikiIngestionJob.

After refactoring, the job delegates all MediaWiki API logic to
MediaWikiReader.  These tests mock the reader's public API
(list_resources, get_resources_info, load_resource) and verify job
orchestration, metadata, and naming.

API/parsing/error-handling coverage now lives in the MediaWikiReader test
suite (~/git/MediaWikiReader/tests/test_mediawiki_reader.py).
"""

import unittest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from llama_index.core.schema import Document

from tasks.mediawiki_ingestion import MediaWikiIngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _default_config(**overrides):
    """Return a minimal config dict for the job."""
    cfg = {"api_url": "https://example.com/w/api.php"}
    cfg.update(overrides)
    return {"name": "test_wiki", "config": cfg}


def _make_job(config=None, **reader_attrs):
    """Create a MediaWikiIngestionJob with a mocked reader.

    ``reader_attrs`` are set as attributes on the mock reader.
    """
    config = config or _default_config()
    with patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader:
        mock_reader = Mock()
        # Sensible defaults
        mock_reader.api_url = config["config"]["api_url"]
        mock_reader.request_delay = config["config"].get("request_delay", 0.1)
        mock_reader.batch_size = config["config"].get("batch_size", 50)
        for k, v in reader_attrs.items():
            setattr(mock_reader, k, v)
        MockReader.return_value = mock_reader
        job = MediaWikiIngestionJob(config)
    return job, mock_reader


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestInitialization(unittest.TestCase):

    def test_creates_reader_with_config(self):
        """Reader should receive the config values from the job config."""
        cfg = _default_config(
            user_agent="test/1.0",
            request_delay=0.5,
            page_limit=100,
            batch_size=10,
            max_retries=5,
            timeout=60,
            namespaces=[0, 1],
        )
        with patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader:
            MockReader.return_value = Mock(api_url="https://example.com/w/api.php",
                                          request_delay=0.5, batch_size=10)
            MediaWikiIngestionJob(cfg)
            MockReader.assert_called_once_with(
                api_url="https://example.com/w/api.php",
                user_agent="test/1.0",
                request_delay=0.5,
                page_limit=100,
                batch_size=10,
                max_retries=5,
                timeout=60,
                namespaces=[0, 1],
                schedules=3600,
            )

    def test_missing_api_url_raises(self):
        """Reader should raise ValueError if api_url is empty/missing."""
        with patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader:
            MockReader.side_effect = ValueError("api_url is required")
            with self.assertRaises(ValueError):
                MediaWikiIngestionJob(_default_config(api_url=""))

    def test_source_type(self):
        job, _ = _make_job()
        self.assertEqual(job.source_type, "mediawiki")


# ---------------------------------------------------------------------------
# list_items
# ---------------------------------------------------------------------------

class TestListItems(unittest.TestCase):

    @patch("tasks.mediawiki_ingestion.time.sleep")
    def test_list_items_basic(self, mock_sleep):
        """Pages returned from the reader's generator → IngestionItems."""
        job, reader = _make_job()
        reader._get_all_pages_generator.return_value = [
            {"title": "Page 1", "last_modified": datetime(2024, 1, 1), "url": "u1"},
            {"title": "Page 2", "last_modified": datetime(2024, 1, 2), "url": "u2"},
        ]

        items = list(job.list_items())

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].id, "mediawiki:Page 1")
        self.assertEqual(items[0].source_ref, "Page 1")
        self.assertEqual(items[0].last_modified, datetime(2024, 1, 1))
        self.assertEqual(items[1].id, "mediawiki:Page 2")

        # reader._get_all_pages_generator called once
        reader._get_all_pages_generator.assert_called_once()
        # get_resources_info and list_resources are NOT called anymore for listing
        reader.list_resources.assert_not_called()
        reader.get_resources_info.assert_not_called()

    @patch("tasks.mediawiki_ingestion.time.sleep")
    def test_list_items_empty_wiki(self, mock_sleep):
        """No pages → no items."""
        job, reader = _make_job()
        reader._get_all_pages_generator.return_value = []

        items = list(job.list_items())
        self.assertEqual(items, [])


# ---------------------------------------------------------------------------
# get_raw_content
# ---------------------------------------------------------------------------

class TestGetRawContent(unittest.TestCase):

    def test_success(self):
        job, reader = _make_job()
        doc = Document(
            text="Clean content",
            metadata={"url": "https://example.com/wiki/P", "title": "P"},
        )
        reader.load_resource.return_value = [doc]

        item = IngestionItem(id="mediawiki:P", source_ref="P")
        content = job.get_raw_content(item)

        self.assertEqual(content, "Clean content")
        self.assertEqual(item._metadata_cache["page_url"], "https://example.com/wiki/P")
        reader.load_resource.assert_called_once_with("P")

    def test_missing_page(self):
        job, reader = _make_job()
        reader.load_resource.return_value = []

        item = IngestionItem(id="mediawiki:M", source_ref="M")
        content = job.get_raw_content(item)

        self.assertEqual(content, "")
        self.assertNotIn("page_url", item._metadata_cache)


# ---------------------------------------------------------------------------
# get_item_name
# ---------------------------------------------------------------------------

class TestGetItemName(unittest.TestCase):

    def test_basic(self):
        job, _ = _make_job()
        item = IngestionItem(id="mediawiki:Test Page", source_ref="Test Page")
        self.assertEqual(job.get_item_name(item), "Test_Page")

    def test_special_characters(self):
        job, _ = _make_job()
        item = IngestionItem(
            id="mediawiki:Page/With:Special*Chars?",
            source_ref="Page/With:Special*Chars?",
        )
        self.assertEqual(job.get_item_name(item), "Page_With_Special_Chars")

    def test_long_title(self):
        job, _ = _make_job()
        long_title = "A" * 300
        item = IngestionItem(id=f"mediawiki:{long_title}", source_ref=long_title)
        result = job.get_item_name(item)
        self.assertEqual(len(result), 255)
        self.assertTrue(result.endswith("A"))

    def test_unicode(self):
        job, _ = _make_job()
        title = "Página_tëst_中文_🚀"
        item = IngestionItem(id=f"mediawiki:{title}", source_ref=title)
        self.assertEqual(job.get_item_name(item), "Página_tëst_中文")

    def test_leading_trailing_underscores(self):
        job, _ = _make_job()
        item = IngestionItem(id="mediawiki:_Test_Page_", source_ref="_Test_Page_")
        self.assertEqual(job.get_item_name(item), "Test_Page")


# ---------------------------------------------------------------------------
# get_document_metadata
# ---------------------------------------------------------------------------

class TestGetDocumentMetadata(unittest.TestCase):

    def test_with_cached_url(self):
        job, _ = _make_job()
        item = IngestionItem(
            id="mediawiki:Test Page",
            source_ref="Test Page",
            last_modified=datetime(2024, 1, 1, 12, 0, 0),
        )
        object.__setattr__(
            item, "_metadata_cache",
            {"page_url": "https://example.com/wiki/Test_Page"},
        )

        metadata = job.get_document_metadata(
            item=item,
            item_name="Test_Page.md",
            checksum="abc123",
            version=1,
            last_modified=datetime(2024, 1, 1, 12, 0, 0),
        )

        self.assertEqual(metadata["source"], "mediawiki")
        self.assertEqual(metadata["key"], "Test_Page.md")
        self.assertEqual(metadata["checksum"], "abc123")
        self.assertEqual(metadata["version"], 1)
        self.assertEqual(metadata["url"], "https://example.com/wiki/Test_Page")

    def test_without_cached_url(self):
        job, _ = _make_job()
        item = IngestionItem(
            id="mediawiki:Test Page",
            source_ref="Test Page",
            last_modified=datetime(2024, 1, 1, 12, 0, 0),
        )

        metadata = job.get_document_metadata(
            item=item,
            item_name="Test_Page.md",
            checksum="abc123",
            version=1,
            last_modified=datetime(2024, 1, 1, 12, 0, 0),
        )

        self.assertEqual(metadata["source"], "mediawiki")
        self.assertNotIn("url", metadata)


# ---------------------------------------------------------------------------
# process_item
# ---------------------------------------------------------------------------

class TestProcessItem(unittest.TestCase):

    @patch("tasks.mediawiki_ingestion.time.sleep")
    def test_success(self, mock_sleep):
        job, reader = _make_job()
        reader.request_delay = 0.1

        doc = Document(
            text="Content",
            metadata={"url": "https://example.com/wiki/P", "title": "P"},
        )
        reader.load_resource.return_value = [doc]

        with patch.object(job.metadata_tracker, "get_latest_record", return_value=None):
            with patch.object(job.metadata_tracker, "record_metadata"):
                with patch.object(job.metadata_tracker, "delete_previous_embeddings"):
                    job.vector_manager.insert_documents = Mock()

                    item = IngestionItem(
                        id="mediawiki:P",
                        source_ref="P",
                        last_modified=datetime(2024, 1, 1),
                    )
                    result = job.process_item(item)

                    self.assertEqual(result, 1)
                    job.metadata_tracker.record_metadata.assert_called_once()
                    job.vector_manager.insert_documents.assert_called_once()

    @patch("tasks.mediawiki_ingestion.time.sleep")
    def test_duplicate_content(self, mock_sleep):
        job, reader = _make_job()
        reader.request_delay = 0.1

        doc = Document(
            text="Duplicate",
            metadata={"url": "https://example.com/wiki/P", "title": "P"},
        )
        reader.load_resource.return_value = [doc]

        with patch.object(job.metadata_tracker, "get_latest_record", return_value=None):
            with patch.object(job.metadata_tracker, "record_metadata"):
                with patch.object(job.metadata_tracker, "delete_previous_embeddings"):
                    job.vector_manager.insert_documents = Mock()
                    job._seen_add = Mock(return_value=False)  # duplicate

                    item = IngestionItem(
                        id="mediawiki:P",
                        source_ref="P",
                        last_modified=datetime(2024, 1, 1),
                    )
                    result = job.process_item(item)

                    self.assertEqual(result, 0)
                    job.metadata_tracker.record_metadata.assert_not_called()
                    job.vector_manager.insert_documents.assert_not_called()


# ---------------------------------------------------------------------------
# Session lifecycle
# ---------------------------------------------------------------------------

class TestSessionLifecycle(unittest.TestCase):

    def test_close_delegates_to_reader(self):
        job, reader = _make_job()
        job.close()
        reader.close.assert_called_once()

    def test_context_manager(self):
        job, reader = _make_job()
        with job:
            pass
        reader.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
