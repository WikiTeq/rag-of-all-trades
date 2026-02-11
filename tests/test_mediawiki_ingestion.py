"""Tests for MediaWikiIngestionJob (Pytest version)."""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch

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


@pytest.fixture
def base_wiki_job():
    """Provide a MediaWikiIngestionJob and its mock reader."""
    return _make_job()


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestInitialization:

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
            )

    def test_missing_api_url_raises(self):
        """Job raises ValueError when api_url is empty (validated in __init__ before reader build).
        Boundary: real MediaWikiReader validation is in the reader's own test suite."""
        with pytest.raises(ValueError, match="api_url is required"):
            MediaWikiIngestionJob(_default_config(api_url=""))

    def test_source_type(self, base_wiki_job):
        job, _ = base_wiki_job
        assert job.source_type == "mediawiki"


# ---------------------------------------------------------------------------
# list_items
# ---------------------------------------------------------------------------

class TestListItems:

    def test_list_items_basic(self, base_wiki_job):
        """Pages returned from the reader's generator → IngestionItems."""
        job, reader = base_wiki_job
        reader._get_all_pages_generator.return_value = [
            {"title": "Page 1", "last_modified": datetime(2024, 1, 1), "url": "u1"},
            {"title": "Page 2", "last_modified": datetime(2024, 1, 2), "url": "u2"},
        ]

        items = list(job.list_items())

        assert len(items) == 2
        assert items[0].id == "mediawiki:Page 1"
        assert items[0].source_ref == "Page 1"
        assert items[0].last_modified == datetime(2024, 1, 1)
        assert items[0].url == "u1"
        assert items[1].id == "mediawiki:Page 2"
        assert items[1].url == "u2"

        # reader._get_all_pages_generator called once
        reader._get_all_pages_generator.assert_called_once()
        # get_resources_info and list_resources are NOT called anymore for listing
        reader.list_resources.assert_not_called()
        reader.get_resources_info.assert_not_called()

    def test_list_items_empty_wiki(self, base_wiki_job):
        """No pages → no items."""
        job, reader = base_wiki_job
        reader._get_all_pages_generator.return_value = []

        items = list(job.list_items())
        assert items == []


# ---------------------------------------------------------------------------
# get_raw_content
# ---------------------------------------------------------------------------

class TestGetRawContent:

    def test_success(self, base_wiki_job):
        job, reader = base_wiki_job
        doc = Document(
            text="Clean content",
            metadata={"url": "https://example.com/wiki/P", "title": "P"},
        )
        reader.load_resource.return_value = [doc]

        item = IngestionItem(id="mediawiki:P", source_ref="P")
        content = job.get_raw_content(item)

        assert content == "Clean content"
        reader.load_resource.assert_called_once_with(
            "P", resource_url=None, last_modified=None
        )

    def test_missing_page(self, base_wiki_job):
        job, reader = base_wiki_job
        reader.load_resource.return_value = []

        item = IngestionItem(id="mediawiki:M", source_ref="M")
        content = job.get_raw_content(item)

        assert content == ""


# ---------------------------------------------------------------------------
# get_item_name
# ---------------------------------------------------------------------------

class TestGetItemName:

    def test_basic(self, base_wiki_job):
        job, _ = base_wiki_job
        item = IngestionItem(id="mediawiki:Test Page", source_ref="Test Page")
        assert job.get_item_name(item) == "Test_Page"

    def test_special_characters(self, base_wiki_job):
        job, _ = base_wiki_job
        item = IngestionItem(
            id="mediawiki:Page/With:Special*Chars?",
            source_ref="Page/With:Special*Chars?",
        )
        # Colon -> __, slash -> _ so "Page/One" and "Page:One" do not collide
        assert job.get_item_name(item) == "Page_With__Special_Chars"

    def test_long_title(self, base_wiki_job):
        job, _ = base_wiki_job
        long_title = "A" * 300
        item = IngestionItem(id=f"mediawiki:{long_title}", source_ref=long_title)
        result = job.get_item_name(item)
        assert len(result) == 255
        assert result.endswith("A")

    def test_unicode(self, base_wiki_job):
        job, _ = base_wiki_job
        title = "Página_tëst_中文_🚀"
        item = IngestionItem(id=f"mediawiki:{title}", source_ref=title)
        assert job.get_item_name(item) == "Página_tëst_中文"

    def test_leading_trailing_underscores(self, base_wiki_job):
        job, _ = base_wiki_job
        item = IngestionItem(id="mediawiki:_Test_Page_", source_ref="_Test_Page_")
        assert job.get_item_name(item) == "Test_Page"


# ---------------------------------------------------------------------------
# get_document_metadata
# ---------------------------------------------------------------------------

class TestGetExtraMetadata:

    def test_with_cached_url(self, base_wiki_job):
        job, _ = base_wiki_job
        item = IngestionItem(
            id="mediawiki:Test Page",
            source_ref="Test Page",
            last_modified=datetime(2024, 1, 1, 12, 0, 0),
            url="https://example.com/wiki/Test_Page",
        )

        extra = job.get_extra_metadata(
            item=item,
            content="content",
            metadata={},
        )

        assert extra["url"] == "https://example.com/wiki/Test_Page"

    def test_without_cached_url(self, base_wiki_job):
        job, _ = base_wiki_job
        item = IngestionItem(
            id="mediawiki:Test Page",
            source_ref="Test Page",
            last_modified=datetime(2024, 1, 1, 12, 0, 0),
        )

        extra = job.get_extra_metadata(
            item=item,
            content="content",
            metadata={},
        )

        assert "url" not in extra


# ---------------------------------------------------------------------------
# process_item
# ---------------------------------------------------------------------------

class TestProcessItem:

    def test_success(self, base_wiki_job):
        job, reader = base_wiki_job
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
                        url="https://example.com/wiki/P",
                    )
                    result = job.process_item(item)

                    assert result == 1
                    job.metadata_tracker.record_metadata.assert_called_once()
                    job.vector_manager.insert_documents.assert_called_once()

    def test_duplicate_content(self, base_wiki_job):
        job, reader = base_wiki_job
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
                        url="https://example.com/wiki/P",
                    )
                    result = job.process_item(item)

                    assert result == 0
                    job.metadata_tracker.record_metadata.assert_not_called()
                    job.vector_manager.insert_documents.assert_not_called()


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

class TestRun:

    @patch("tasks.base.time.sleep")
    def test_run_applies_delay(self, mock_sleep):
        """run() should call time.sleep with request_delay for each item."""
        cfg = _default_config(request_delay=2.0)
        job, _ = _make_job(config=cfg)

        with patch.object(job, "list_items") as mock_list:
            mock_list.return_value = [
                IngestionItem(id="1", source_ref="1"),
                IngestionItem(id="2", source_ref="2"),
            ]
            with patch.object(job, "process_item", return_value=1):
                job.run()

        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(2.0)
