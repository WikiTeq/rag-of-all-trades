"""Tests for MediaWikiIngestionJob (Pytest version)."""

import sys
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, Mock, patch

import pytest
from llama_index.core.schema import Document
from requests.adapters import HTTPAdapter

# llama-index-readers-mediawiki is not yet published; stub it so the module
# can be imported and MediaWikiReader is always patched in tests.
sys.modules.setdefault("llama_index.readers.mediawiki", MagicMock())

from tasks.helper_classes.ingestion_item import IngestionItem  # noqa: E402
from tasks.mediawiki_ingestion import HostOverrideAdapter, MediaWikiIngestionJob  # noqa: E402

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

    def test_verify_ssl_default_true(self):
        """SSL verification is enabled by default; no custom Site is injected."""
        cfg = _default_config(host="example.com")
        with (
            patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader,
            patch("tasks.mediawiki_ingestion.mwclient.Site") as MockSite,
        ):
            MockReader.return_value = Mock(host="example.com", path="/w/", scheme="https")
            job = MediaWikiIngestionJob(cfg)
            assert job.verify_ssl is True
            MockSite.assert_not_called()

    def test_verify_ssl_disabled_injects_site(self):
        """verify_ssl=False builds a custom mwclient Site with verify=False."""
        cfg = _default_config(host="example.com", verify_ssl=False)
        with (
            patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader,
            patch("tasks.mediawiki_ingestion.mwclient.Site") as MockSite,
            patch("tasks.mediawiki_ingestion.requests.Session") as MockSession,
        ):
            mock_reader = Mock(host="example.com", path="/w/", scheme="https")
            MockReader.return_value = mock_reader
            mock_session = Mock()
            mock_session.headers = {}
            MockSession.return_value = mock_session

            job = MediaWikiIngestionJob(cfg)

            assert job.verify_ssl is False
            assert mock_session.verify is False
            MockSite.assert_called_once()
            call_kwargs = MockSite.call_args.kwargs
            assert call_kwargs["pool"] is mock_session
            assert call_kwargs["connection_options"] == {"verify": False}
            assert mock_reader._site is MockSite.return_value

    def test_verify_ssl_string_false(self):
        """String 'false' from env interpolation is parsed as False."""
        cfg = _default_config(host="example.com", verify_ssl="false")
        with (
            patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader,
            patch("tasks.mediawiki_ingestion.mwclient.Site"),
            patch("tasks.mediawiki_ingestion.requests.Session") as MockSession,
        ):
            MockReader.return_value = Mock(host="example.com", path="/w/", scheme="https")
            mock_session = Mock()
            mock_session.headers = {}
            MockSession.return_value = mock_session

            job = MediaWikiIngestionJob(cfg)
            assert job.verify_ssl is False

    def test_resolve_to_ip_mounts_host_override_adapter(self):
        """resolve_to_ip mounts HostOverrideAdapter on the session."""
        cfg = _default_config(host="wiki.example.com", resolve_to_ip="10.0.0.1")
        with (
            patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader,
            patch("tasks.mediawiki_ingestion.mwclient.Site") as MockSite,
            patch("tasks.mediawiki_ingestion.requests.Session") as MockSession,
        ):
            MockReader.return_value = Mock(host="wiki.example.com", path="/w/", scheme="https")
            mock_session = Mock()
            mock_session.headers = {}
            MockSession.return_value = mock_session

            MediaWikiIngestionJob(cfg)

            mock_session.mount.assert_called_once()
            prefix, adapter = mock_session.mount.call_args[0]
            assert prefix == "https://wiki.example.com"
            assert isinstance(adapter, HostOverrideAdapter)
            assert adapter._dest_ip == "10.0.0.1"
            assert adapter._dest_hostname == "wiki.example.com"
            MockSite.assert_called_once()

    def test_resolve_to_ip_from_api_url(self):
        """resolve_to_ip uses hostname parsed from api_url."""
        cfg = _default_config(
            api_url="https://wiki.example.com/w/api.php",
            resolve_to_ip="10.0.0.1",
        )
        with (
            patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader,
            patch("tasks.mediawiki_ingestion.mwclient.Site"),
            patch("tasks.mediawiki_ingestion.requests.Session") as MockSession,
        ):
            MockReader.return_value = Mock(host="wiki.example.com", path="/w/", scheme="https")
            mock_session = Mock()
            mock_session.headers = {}
            MockSession.return_value = mock_session

            MediaWikiIngestionJob(cfg)

            prefix, _ = mock_session.mount.call_args[0]
            assert prefix == "https://wiki.example.com"

    def test_custom_headers_applied_to_session(self):
        """custom_headers are merged into the session used by mwclient."""
        cfg = _default_config(
            host="example.com",
            custom_headers={"Authorization": "Bearer token123", "X-Custom": "value"},
        )
        with (
            patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader,
            patch("tasks.mediawiki_ingestion.mwclient.Site"),
            patch("tasks.mediawiki_ingestion.requests.Session") as MockSession,
        ):
            MockReader.return_value = Mock(host="example.com", path="/w/", scheme="https")
            mock_session = Mock()
            mock_session.headers = {}
            MockSession.return_value = mock_session

            MediaWikiIngestionJob(cfg)

            assert mock_session.headers["Authorization"] == "Bearer token123"
            assert mock_session.headers["X-Custom"] == "value"
            # mwclient default UA restored when using a custom pool
            assert "User-Agent" in mock_session.headers

    def test_custom_headers_ignored_when_not_dict(self):
        """Non-dict custom_headers are ignored without raising."""
        cfg = _default_config(host="example.com", custom_headers="not-a-dict")
        with (
            patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader,
            patch("tasks.mediawiki_ingestion.mwclient.Site") as MockSite,
        ):
            MockReader.return_value = Mock(host="example.com", path="/w/", scheme="https")
            job = MediaWikiIngestionJob(cfg)
            assert job is not None
            # No network overrides → Site not built eagerly
            MockSite.assert_not_called()

    def test_user_agent_override(self):
        """user_agent sets the session User-Agent and injects a custom Site."""
        ua = "Mozilla/5.0 (compatible; RAGacy-test/1.0)"
        cfg = _default_config(host="example.com", user_agent=ua)
        with (
            patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader,
            patch("tasks.mediawiki_ingestion.mwclient.Site") as MockSite,
            patch("tasks.mediawiki_ingestion.requests.Session") as MockSession,
        ):
            MockReader.return_value = Mock(host="example.com", path="/w/", scheme="https")
            mock_session = Mock()
            mock_session.headers = {}
            MockSession.return_value = mock_session

            MediaWikiIngestionJob(cfg)

            assert mock_session.headers["User-Agent"] == ua
            MockSite.assert_called_once()

    def test_user_agent_wins_over_custom_headers(self):
        """Dedicated user_agent config overrides User-Agent from custom_headers."""
        ua = "RAGacy-connector/1.0"
        cfg = _default_config(
            host="example.com",
            user_agent=ua,
            custom_headers={"User-Agent": "should-not-win", "X-Custom": "ok"},
        )
        with (
            patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader,
            patch("tasks.mediawiki_ingestion.mwclient.Site"),
            patch("tasks.mediawiki_ingestion.requests.Session") as MockSession,
        ):
            MockReader.return_value = Mock(host="example.com", path="/w/", scheme="https")
            mock_session = Mock()
            mock_session.headers = {}
            MockSession.return_value = mock_session

            MediaWikiIngestionJob(cfg)

            assert mock_session.headers["User-Agent"] == ua
            assert mock_session.headers["X-Custom"] == "ok"

    def test_no_custom_site_when_defaults(self):
        """Default network options leave MediaWikiReader's Site creation alone."""
        cfg = _default_config(host="example.com")
        with (
            patch("tasks.mediawiki_ingestion.MediaWikiReader") as MockReader,
            patch("tasks.mediawiki_ingestion.mwclient.Site") as MockSite,
        ):
            MockReader.return_value = Mock(host="example.com", path="/w/", scheme="https")
            MediaWikiIngestionJob(cfg)
            MockSite.assert_not_called()


# ---------------------------------------------------------------------------
# HostOverrideAdapter
# ---------------------------------------------------------------------------


class TestHostOverrideAdapter:
    def test_send_rewrites_url_to_ip(self):
        """Hostname is replaced with the override IP; Host header is preserved."""
        adapter = HostOverrideAdapter(dest_ip="10.0.0.1", dest_hostname="wiki.example.com")

        mock_request = Mock()
        mock_request.url = "https://wiki.example.com/w/api.php?action=query"
        mock_request.headers = {}

        with patch.object(HTTPAdapter, "send", return_value=Mock()) as mock_super_send:
            adapter.send(mock_request)

            assert "10.0.0.1" in mock_request.url
            assert "wiki.example.com" not in mock_request.url
            assert mock_request.headers["Host"] == "wiki.example.com"
            mock_super_send.assert_called_once()

    def test_send_preserves_existing_host_header(self):
        """Existing Host header is not overwritten."""
        adapter = HostOverrideAdapter(dest_ip="10.0.0.1", dest_hostname="wiki.example.com")

        mock_request = Mock()
        mock_request.url = "https://wiki.example.com/w/api.php"
        mock_request.headers = {"Host": "custom-host.example.com"}

        with patch.object(HTTPAdapter, "send", return_value=Mock()):
            adapter.send(mock_request)
            assert mock_request.headers["Host"] == "custom-host.example.com"

    def test_init_poolmanager_sets_server_hostname(self):
        """init_poolmanager passes server_hostname for TLS SNI."""
        adapter = HostOverrideAdapter(dest_ip="10.0.0.1", dest_hostname="wiki.example.com")

        with patch.object(HTTPAdapter, "init_poolmanager") as mock_super_init:
            adapter.init_poolmanager(1, 10)
            mock_super_init.assert_called_once_with(1, 10, server_hostname="wiki.example.com")


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
