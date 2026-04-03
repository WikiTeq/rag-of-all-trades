"""Tests for MediaWikiIngestionJob (Pytest version)."""

import sys
from unittest.mock import MagicMock

# llama-index-readers-mediawiki is not yet published; stub it so the module
# can be imported and MediaWikiReader is always patched in tests.
sys.modules.setdefault("llama_index.readers.mediawiki", MagicMock())

from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from llama_index.core.schema import Document

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.mediawiki_ingestion import MediaWikiIngestionJob

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _default_config(**overrides):
    """Return a minimal config dict for the job."""
    cfg = {"host": "example.com"}
    cfg.update(overrides)
    return {"name": "test_wiki", "config": cfg}


def _make_job(config=None, **reader_attrs):
    """Create a MediaWikiIngestionJob with a mocked reader.

    ``reader_attrs`` are set as attributes on the mock reader.
    """
    config = config or _default_config()
    with patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader:
        mock_reader = Mock()
        # Sensible defaults matching reader Pydantic fields
        mock_reader.host = config["config"]["host"]
        mock_reader.path = config["config"].get("path", "/w/")
        mock_reader.scheme = config["config"].get("scheme", "https")
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
            path="/wiki/",
            scheme="http",
            page_limit=100,
            namespaces=[0, 1],
            filter_redirects=False,
        )
        with patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader:
            MockReader.return_value = Mock(host="example.com", path="/wiki/", scheme="http")
            MediaWikiIngestionJob(cfg)
            MockReader.assert_called_once_with(
                host="example.com",
                path="/wiki/",
                scheme="http",
                page_limit=100,
                namespaces=[0, 1],
                filter_redirects=False,
            )

    def test_missing_host_raises(self):
        """Job raises ValueError when host is empty."""
        with pytest.raises(ValueError, match="host is required"):
            MediaWikiIngestionJob(_default_config(host=""))

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
            {"title": "Page 1", "last_modified": datetime(2024, 1, 1), "url": "u1", "pageid": 1, "namespace": 0},
            {"title": "Page 2", "last_modified": datetime(2024, 1, 2), "url": "u2", "pageid": 2, "namespace": 4},
        ]

        items = list(job.list_items())

        assert len(items) == 2
        assert items[0].id == "mediawiki:Page 1"
        assert items[0].source_ref == "Page 1"
        assert items[0].last_modified == datetime(2024, 1, 1)
        assert items[0].url == "u1"
        assert items[0].pageid == 1
        assert items[0].namespace == 0
        assert items[1].id == "mediawiki:Page 2"
        assert items[1].url == "u2"
        assert items[1].pageid == 2
        assert items[1].namespace == 4

        reader._get_all_pages_generator.assert_called_once()

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
        reader._page_to_document.return_value = doc

        item = IngestionItem(id="mediawiki:P", source_ref="P", pageid=42, namespace=0)
        content = job.get_raw_content(item)

        assert content == "Clean content"
        reader._page_to_document.assert_called_once_with(
            title="P",
            url=None,
            last_modified=None,
            pageid=42,
            namespace=0,
        )

    def test_missing_page(self, base_wiki_job):
        job, reader = base_wiki_job
        reader._page_to_document.return_value = None

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
# get_extra_metadata
# ---------------------------------------------------------------------------


class TestGetExtraMetadata:
    def test_with_cached_url(self, base_wiki_job):
        job, _ = base_wiki_job
        item = IngestionItem(
            id="mediawiki:Test Page",
            source_ref="Test Page",
            last_modified=datetime(2024, 1, 1, 12, 0, 0),
            url="https://example.com/wiki/Test_Page",
            pageid=10,
            namespace=0,
        )

        extra = job.get_extra_metadata(
            item=item,
            content="content",
            metadata={},
        )

        assert extra["url"] == "https://example.com/wiki/Test_Page"
        assert extra["title"] == "Test Page"
        assert extra["page_id"] == 10
        assert extra["namespace"] == 0

    def test_without_cached_url(self, base_wiki_job):
        job, _ = base_wiki_job
        item = IngestionItem(
            id="mediawiki:Test Page",
            source_ref="Test Page",
            last_modified=datetime(2024, 1, 1, 12, 0, 0),
            pageid=10,
            namespace=0,
        )

        extra = job.get_extra_metadata(
            item=item,
            content="content",
            metadata={},
        )

        assert "url" not in extra
        assert extra["page_id"] == 10
        assert extra["namespace"] == 0


# ---------------------------------------------------------------------------
# process_item
# ---------------------------------------------------------------------------


class TestProcessItem:
    def test_success(self, base_wiki_job):
        job, reader = base_wiki_job

        doc = Document(
            text="Content",
            metadata={"url": "https://example.com/wiki/P", "title": "P"},
        )
        reader._page_to_document.return_value = doc

        with patch.object(job.metadata_tracker, "get_latest_record", return_value=None):
            with patch.object(job.metadata_tracker, "record_metadata"):
                with patch.object(job.metadata_tracker, "delete_previous_embeddings"):
                    job.vector_manager.insert_documents = Mock()

                    item = IngestionItem(
                        id="mediawiki:P",
                        source_ref="P",
                        last_modified=datetime(2024, 1, 1),
                        url="https://example.com/wiki/P",
                        pageid=1,
                        namespace=0,
                    )
                    result = job.process_item(item)

                    assert result == 1
                    job.metadata_tracker.record_metadata.assert_called_once()
                    job.vector_manager.insert_documents.assert_called_once()

    def test_duplicate_content(self, base_wiki_job):
        job, reader = base_wiki_job

        doc = Document(
            text="Duplicate",
            metadata={"url": "https://example.com/wiki/P", "title": "P"},
        )
        reader._page_to_document.return_value = doc

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
        cfg = _default_config()
        cfg["config"]["request_delay"] = 2.0
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
