"""Tests for MediaWikiIngestionJob (Pytest version)."""

import json
import sys
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, Mock, patch

import pytest
from llama_index.core.schema import Document

# llama-index-readers-mediawiki is not yet published; stub it so the module
# can be imported and MediaWikiReader is always patched in tests.
sys.modules.setdefault("llama_index.readers.mediawiki", MagicMock())

from tasks.helper_classes.ingestion_item import IngestionItem  # noqa: E402
from tasks.mediawiki_ingestion import MediaWikiIngestionJob  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _default_config(**overrides):
    """Return a minimal config dict for the job."""
    cfg = {}
    cfg.update(overrides)
    return {"name": "test_wiki", "config": cfg}


def _make_job(config=None, **reader_attrs):
    """Create a MediaWikiIngestionJob with a mocked reader.

    ``reader_attrs`` are set as attributes on the mock reader.
    """
    config = config or _default_config(host="example.com")
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


def _make_page(title, last_modified=None, url=None, pageid=1, namespace=0, revision=12345):
    """Return a SimpleNamespace mimicking a MediaWiki page record."""
    return SimpleNamespace(
        title=title,
        last_modified=last_modified,
        url=url,
        pageid=pageid,
        namespace=namespace,
        revision=revision,
    )


def _make_item(title, last_modified=None, **page_kwargs):
    """Return an IngestionItem with a page_record in source_ref."""
    page_record = _make_page(title, last_modified=last_modified, **page_kwargs)
    return IngestionItem(
        id=f"mediawiki:{title}",
        source_ref=page_record,
        last_modified=last_modified,
    )


@pytest.fixture
def base_wiki_job():
    """Provide a MediaWikiIngestionJob and its mock reader."""
    return _make_job()


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestInitialization:
    def test_creates_reader_with_config_host_path_scheme(self):
        """Reader should receive the config values from the job config."""
        cfg = _default_config(
            host="example.com",
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
                logger=ANY,
            )

    def test_creates_reader_with_api_url(self):
        """Reader should receive the config values from the job config."""
        cfg = _default_config(
            api_url="https://example.com/w/api.php",
            page_limit=100,
            namespaces=[0, 1],
            filter_redirects=False,
        )
        with patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader:
            MockReader.return_value = Mock(api_url="https://example.com/w/api.php")
            MediaWikiIngestionJob(cfg)
            MockReader.assert_called_once_with(
                host="example.com",
                path="/w/",
                scheme="https",
                page_limit=100,
                namespaces=[0, 1],
                filter_redirects=False,
                logger=ANY,
            )

    def test_namespaces_int_converted_to_list(self):
        """Single int namespace should be wrapped in a list."""
        cfg = _default_config(host="example.com", namespaces=0)
        with patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader:
            MockReader.return_value = Mock(host="example.com", path="/w/", scheme="https")
            MediaWikiIngestionJob(cfg)
            _, kwargs = MockReader.call_args
            assert kwargs["namespaces"] == [0]

    def test_namespaces_str_converted_to_list(self):
        """Comma-separated string namespace should be converted to a list of ints."""
        cfg = _default_config(host="example.com", namespaces="0,1")
        with patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader:
            MockReader.return_value = Mock(host="example.com", path="/w/", scheme="https")
            MediaWikiIngestionJob(cfg)
            _, kwargs = MockReader.call_args
            assert kwargs["namespaces"] == [0, 1]

    def test_namespaces_list_passthrough(self):
        """List namespace should be passed through unchanged."""
        cfg = _default_config(host="example.com", namespaces=[0, 1, 4])
        with patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader:
            MockReader.return_value = Mock(host="example.com", path="/w/", scheme="https")
            MediaWikiIngestionJob(cfg)
            _, kwargs = MockReader.call_args
            assert kwargs["namespaces"] == [0, 1, 4]

    def test_namespaces_none_passthrough(self):
        """Absent namespaces should pass None to the reader (default content namespaces)."""
        cfg = _default_config(host="example.com")
        with patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader:
            MockReader.return_value = Mock(host="example.com", path="/w/", scheme="https")
            MediaWikiIngestionJob(cfg)
            _, kwargs = MockReader.call_args
            assert kwargs["namespaces"] is None

    def test_missing_host_and_api_url_raises(self):
        """Job raises ValueError when host is empty."""
        with pytest.raises(ValueError, match="is required"):
            MediaWikiIngestionJob(_default_config(host=""))

    def test_host_and_api_url_raises(self):
        """Job raises ValueError when host is empty."""
        with pytest.raises(ValueError, match="Only one of"):
            MediaWikiIngestionJob(_default_config(host="123", api_url="123"))

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
        page1 = _make_page("Page 1", last_modified=datetime(2024, 1, 1), url="u1", pageid=1, namespace=0)
        page2 = _make_page("Page 2", last_modified=datetime(2024, 1, 2), url="u2", pageid=2, namespace=4)
        reader._get_all_pages_generator.return_value = [page1, page2]

        items = list(job.list_items())

        assert len(items) == 2
        assert items[0].id == "mediawiki:Page 1"
        assert items[0].source_ref is page1
        assert items[0].last_modified == datetime(2024, 1, 1)
        assert items[1].id == "mediawiki:Page 2"
        assert items[1].source_ref is page2
        assert items[1].last_modified == datetime(2024, 1, 2)

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

        item = _make_item("P", pageid=42, namespace=0)
        content = job.get_raw_content(item)

        assert content == "Clean content"
        reader._page_to_document.assert_called_once_with(item.source_ref)

    def test_missing_page(self, base_wiki_job):
        job, reader = base_wiki_job
        reader._page_to_document.return_value = None

        item = _make_item("M")
        content = job.get_raw_content(item)

        assert content == ""


# ---------------------------------------------------------------------------
# get_item_name
# ---------------------------------------------------------------------------


class TestGetItemName:
    def test_basic(self, base_wiki_job):
        job, _ = base_wiki_job
        item = _make_item("Test Page")
        assert job.get_item_name(item) == "Test_Page"

    def test_special_characters(self, base_wiki_job):
        job, _ = base_wiki_job
        item = _make_item("Page/With:Special*Chars?")
        # Colon -> __, slash -> _ so "Page/One" and "Page:One" do not collide
        assert job.get_item_name(item) == "Page_With__Special_Chars"

    def test_long_title(self, base_wiki_job):
        job, _ = base_wiki_job
        long_title = "A" * 300
        item = _make_item(long_title)
        result = job.get_item_name(item)
        assert len(result) == 255
        assert result.endswith("A")

    def test_unicode(self, base_wiki_job):
        job, _ = base_wiki_job
        title = "Página_tëst_中文_🚀"
        item = _make_item(title)
        assert job.get_item_name(item) == "Página_tëst_中文"

    def test_leading_trailing_underscores(self, base_wiki_job):
        job, _ = base_wiki_job
        item = _make_item("_Test_Page_")
        assert job.get_item_name(item) == "Test_Page"


# ---------------------------------------------------------------------------
# get_extra_metadata
# ---------------------------------------------------------------------------


class TestGetExtraMetadata:
    def test_with_url(self, base_wiki_job):
        job, _ = base_wiki_job
        item = _make_item(
            "Test Page",
            last_modified=datetime(2024, 1, 1, 12, 0, 0),
            url="https://example.com/wiki/Test_Page",
            pageid=10,
            namespace=0,
        )

        extra = job.get_extra_metadata(item=item, content="content", metadata={})

        assert extra["url"] == "https://example.com/wiki/Test_Page"
        assert extra["title"] == "Test Page"
        assert extra["page_id"] == 10
        assert extra["namespace"] == 0

    def test_without_url(self, base_wiki_job):
        job, _ = base_wiki_job
        item = _make_item("Test Page", last_modified=datetime(2024, 1, 1, 12, 0, 0), pageid=10, namespace=0)

        extra = job.get_extra_metadata(item=item, content="content", metadata={})

        assert "url" not in extra
        assert extra["page_id"] == 10
        assert extra["namespace"] == 0

    def test_load_semantics_disabled_by_default(self):
        job, _ = _make_job()
        assert job.load_semantics is False

    def test_load_semantics_not_queried_when_disabled(self, base_wiki_job):
        job, reader = base_wiki_job
        item = _make_item("Test Page", last_modified=datetime(2024, 1, 1, 12, 0, 0), pageid=10, namespace=0)

        job.get_extra_metadata(item=item, content="content", metadata={})

        reader.site.get.assert_not_called()

    def test_load_semantics_queries_smwbrowse_with_expected_args(self):
        job, reader = _make_job(config=_default_config(host="example.com", load_semantics=True))
        reader.site.get.return_value = {"query": {"data": []}, "meta": {"type": "subject"}}
        item = _make_item("Test Page", last_modified=datetime(2024, 1, 1, 12, 0, 0), pageid=10, namespace=0)

        job.get_extra_metadata(item=item, content="content", metadata={})

        reader.site.get.assert_called_once_with(
            "smwbrowse",
            browse="subject",
            params=ANY,
            format="json",
        )
        _, kwargs = reader.site.get.call_args
        assert json.loads(kwargs["params"]) == {"subject": "Test Page", "ns": 0, "iw": "", "subobject": ""}

    def test_load_semantics_merges_regular_properties(self):
        job, reader = _make_job(config=_default_config(host="example.com", load_semantics=True))
        reader.site.get.return_value = {
            "query": {
                "data": [
                    {"property": "Sitename", "dataitem": [{"type": 2, "item": "Some Value"}], "direction": "direct"},
                    {"property": "Is_discontinued", "dataitem": [{"type": 1, "item": "false"}], "direction": "direct"},
                ],
                "sobj": [{"data": [{"property": "Should_be_ignored", "dataitem": [{"type": 2, "item": "x"}]}]}],
            },
            "meta": {"type": "subject"},
        }
        item = _make_item("Test Page", last_modified=datetime(2024, 1, 1, 12, 0, 0), pageid=10, namespace=0)

        extra = job.get_extra_metadata(item=item, content="content", metadata={})

        assert extra["smw_Sitename"] == "Some Value"
        assert extra["smw_Is_discontinued"] == "false"
        assert "smw_Should_be_ignored" not in extra
        assert "Should_be_ignored" not in extra

    def test_load_semantics_skips_system_properties(self):
        job, reader = _make_job(config=_default_config(host="example.com", load_semantics=True))
        reader.site.get.return_value = {
            "query": {
                "data": [
                    {"property": "_ASK", "dataitem": [{"type": 2, "item": "x"}], "direction": "direct"},
                    {"property": "_INST", "dataitem": [{"type": 2, "item": "y"}], "direction": "direct"},
                    {"property": "_SKEY", "dataitem": [{"type": 2, "item": "z"}], "direction": "direct"},
                    {"property": "Sitename", "dataitem": [{"type": 2, "item": "Some Value"}], "direction": "direct"},
                ]
            }
        }
        item = _make_item("Test Page", last_modified=datetime(2024, 1, 1, 12, 0, 0), pageid=10, namespace=0)

        extra = job.get_extra_metadata(item=item, content="content", metadata={})

        assert "_ASK" not in extra
        assert "_INST" not in extra
        assert "_SKEY" not in extra
        assert "smw__ASK" not in extra
        assert "smw__INST" not in extra
        assert "smw__SKEY" not in extra
        assert extra["smw_Sitename"] == "Some Value"

    def test_load_semantics_takes_first_value_for_multi_valued_property(self):
        job, reader = _make_job(config=_default_config(host="example.com", load_semantics=True))
        reader.site.get.return_value = {
            "query": {
                "data": [
                    {
                        "property": "Tags",
                        "dataitem": [{"type": 2, "item": "first"}, {"type": 2, "item": "second"}],
                        "direction": "direct",
                    }
                ]
            }
        }
        item = _make_item("Test Page", last_modified=datetime(2024, 1, 1, 12, 0, 0), pageid=10, namespace=0)

        extra = job.get_extra_metadata(item=item, content="content", metadata={})

        assert extra["smw_Tags"] == "first"

    def test_load_semantics_decodes_wikipage_type_values(self):
        job, reader = _make_job(config=_default_config(host="example.com", load_semantics=True))
        reader.site.get.return_value = {
            "query": {
                "data": [
                    {
                        "property": "Sitename",
                        "dataitem": [{"type": 9, "item": "Test_Wiki#0##"}],
                        "direction": "direct",
                    },
                    {
                        "property": "Tags",
                        "dataitem": [{"type": 9, "item": "First_tag#0##"}, {"type": 9, "item": "Second_tag#0##"}],
                        "direction": "direct",
                    },
                ]
            }
        }
        item = _make_item("Test Page", last_modified=datetime(2024, 1, 1, 12, 0, 0), pageid=10, namespace=0)

        extra = job.get_extra_metadata(item=item, content="content", metadata={})

        assert extra["smw_Sitename"] == "Test Wiki"
        assert extra["smw_Tags"] == "First tag"

    def test_load_semantics_does_not_clobber_connector_metadata(self):
        job, reader = _make_job(config=_default_config(host="example.com", load_semantics=True))
        reader.site.get.return_value = {
            "query": {
                "data": [
                    {"property": "Title", "dataitem": [{"type": 2, "item": "Fake Title Collision"}]},
                    {"property": "Page_id", "dataitem": [{"type": 2, "item": "999"}]},
                ]
            }
        }
        item = _make_item(
            "Test Page",
            last_modified=datetime(2024, 1, 1, 12, 0, 0),
            pageid=10,
            namespace=0,
            url="https://example.com/wiki/Test_Page",
        )

        extra = job.get_extra_metadata(item=item, content="content", metadata={})

        assert extra["title"] == "Test Page"
        assert extra["page_id"] == 10
        assert extra["smw_Title"] == "Fake Title Collision"
        assert extra["smw_Page_id"] == "999"

    def test_load_semantics_failure_does_not_raise_and_omits_semantic_metadata(self):
        job, reader = _make_job(config=_default_config(host="example.com", load_semantics=True))
        reader.site.get.side_effect = Exception("boom")
        item = _make_item(
            "Test Page",
            last_modified=datetime(2024, 1, 1, 12, 0, 0),
            pageid=10,
            namespace=0,
            url="https://example.com/wiki/Test_Page",
        )

        extra = job.get_extra_metadata(item=item, content="content", metadata={})

        assert extra["title"] == "Test Page"
        assert extra["url"] == "https://example.com/wiki/Test_Page"


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

                    item = _make_item(
                        "P",
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

                    item = _make_item("P", last_modified=datetime(2024, 1, 1))
                    result = job.process_item(item)

                    assert result == 0
                    job.metadata_tracker.record_metadata.assert_not_called()
                    job.vector_manager.insert_documents.assert_not_called()


# ---------------------------------------------------------------------------
# get_item_checksum
# ---------------------------------------------------------------------------


class TestGetItemChecksum:
    @pytest.mark.parametrize(
        "revision,expected",
        [
            (98765, "98765"),
            (0, None),
            (None, None),
        ],
    )
    def test_get_item_checksum(self, base_wiki_job, revision, expected):
        job, _ = base_wiki_job
        item = _make_item("Page", revision=revision)
        assert job.get_item_checksum(item) == expected
