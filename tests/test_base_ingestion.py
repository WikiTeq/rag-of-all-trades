import hashlib
import unittest
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.schemas import BaseMetadataSchema


class DummyIngestionJob(IngestionJob):
    def __init__(self, config, items=None, content_by_id=None, name_by_id=None):
        super().__init__(config)
        self._items = items or []
        self._content_by_id = content_by_id or {}
        self._name_by_id = name_by_id or {}

    @property
    def source_type(self) -> str:
        return "dummy"

    def list_items(self):
        yield from self._items

    def get_raw_content(self, item):
        return self._content_by_id.get(item.id, "")

    def get_item_name(self, item):
        return self._name_by_id.get(item.id, item.id)


@pytest.fixture
def base_config():
    return {"name": "test-source"}


class TestIngestionJob:
    def test_standard_metadata_construction(self, base_config):
        """Standard metadata should be built correctly in process_item."""
        job = DummyIngestionJob(base_config)
        item = IngestionItem(
            id="item-1",
            source_ref="src",
            last_modified=datetime(2024, 1, 1),
        )
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.get_raw_content = Mock(return_value="content")
        job.metadata_tracker.get_latest_record.return_value = None

        job.process_item(item)

        # Verify metadata passed to VectorStore
        args, _ = job.vector_manager.insert_documents.call_args
        metadata = args[0][0].metadata
        assert metadata["source"] == "dummy"
        assert metadata["key"] == "item-1"
        assert metadata["version"] == 1
        assert metadata["last_modified"] == "2024-01-01 00:00:00"

    def test_get_extra_metadata_merge(self, base_config):
        """Extra metadata from hook should be merged into final result."""
        job = DummyIngestionJob(base_config)
        job.get_extra_metadata = Mock(return_value={"custom": "val"})

        item = IngestionItem(id="item-1", source_ref="src")
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.get_raw_content = Mock(return_value="content")
        job.metadata_tracker.get_latest_record.return_value = None

        job.process_item(item)

        args, _ = job.vector_manager.insert_documents.call_args
        metadata = args[0][0].metadata
        assert metadata["custom"] == "val"
        assert metadata["source"] == "dummy"  # Standard still there

    def test_get_extra_metadata_reserved_keys_not_overwritten(self, base_config):
        """Extra metadata must not overwrite reserved standard keys."""
        schema_fields = BaseMetadataSchema.model_fields

        job = DummyIngestionJob(base_config)
        job.get_extra_metadata = Mock(return_value={k: "overwrite" for k in schema_fields})

        item = IngestionItem(id="item-1", source_ref="src")
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.get_raw_content = Mock(return_value="content")
        job.metadata_tracker.get_latest_record.return_value = None

        job.process_item(item)

        args, _ = job.vector_manager.insert_documents.call_args
        metadata = args[0][0].metadata
        for key in schema_fields:
            assert metadata[key] != "overwrite", f"Reserved key {key} was overwritten"
        assert metadata["source"] == "dummy"
        assert metadata["key"] == "item-1"
        assert metadata["version"] == 1

    def test_seen_add_lru_eviction(self, base_config):
        job = DummyIngestionJob(base_config)
        job._seen_capacity = 2

        assert job._seen_add("a") is True
        assert job._seen_add("b") is True
        assert job._seen_add("a") is False
        assert job._seen_add("c") is True
        assert job._seen_add("b") is True

    def test_process_item_skips_empty_content(self, base_config):
        item = IngestionItem(id="item-1", source_ref="src")
        job = DummyIngestionJob(
            base_config,
            items=[item],
            content_by_id={"item-1": "   "},
        )
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()

        result = job.process_item(item)

        assert result == 0
        job.metadata_tracker.get_latest_record.assert_not_called()
        job.vector_manager.insert_documents.assert_not_called()

    def test_process_item_skips_unchanged_content(self, base_config):
        content = "same content"
        checksum = hashlib.md5(content.encode("utf-8"), usedforsecurity=False).hexdigest()
        item = IngestionItem(id="item-1", source_ref="src")
        job = DummyIngestionJob(
            base_config,
            items=[item],
            content_by_id={"item-1": content},
        )
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.metadata_tracker.get_latest_record.return_value = Mock(
            checksum=checksum,
            version=1,
        )

        with patch.object(job, "_seen_add", return_value=True):
            result = job.process_item(item)

        assert result == 0
        job.metadata_tracker.get_latest_record.assert_called_once_with("item-1")
        job.metadata_tracker.delete_previous_embeddings.assert_not_called()
        job.metadata_tracker.record_metadata.assert_not_called()
        job.vector_manager.insert_documents.assert_not_called()

    @patch("tasks.base.Document")
    def test_process_item_updates_version_and_records_metadata(self, mock_document, base_config):
        content = "updated content"
        checksum = hashlib.md5(content.encode("utf-8"), usedforsecurity=False).hexdigest()
        last_modified = datetime(2024, 1, 2, 3, 4, 5)
        item = IngestionItem(
            id="item-1",
            source_ref="src",
            last_modified=last_modified,
        )
        job = DummyIngestionJob(
            base_config,
            items=[item],
            content_by_id={"item-1": content},
        )
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.metadata_tracker.get_latest_record.return_value = Mock(
            checksum="old",
            version=2,
        )

        with patch.object(job, "_seen_add", return_value=True):
            result = job.process_item(item)

        assert result == 1
        job.metadata_tracker.delete_previous_embeddings.assert_called_once_with("item-1")
        job.vector_manager.insert_documents.assert_called_once_with([mock_document.return_value])
        job.metadata_tracker.record_metadata.assert_called_once_with(
            "item-1",
            checksum,
            3,
            1,
            last_modified,
            extra_metadata={"source_name": "test-source"},
        )

        assert mock_document.call_count == 1
        _, kwargs = mock_document.call_args
        assert kwargs["text"] == content
        assert kwargs["metadata"]["checksum"] == checksum
        assert kwargs["metadata"]["version"] == 3
        assert kwargs["metadata"]["source"] == "dummy"

    def test_get_item_checksum_default_returns_none(self, base_config):
        job = DummyIngestionJob(base_config)
        item = IngestionItem(id="item-1", source_ref="src")
        assert job.get_item_checksum(item) is None

    def test_process_item_skips_when_pre_checksum_matches(self, base_config):
        """Pre-checksum matches DB record → get_raw_content is never called."""
        item = IngestionItem(id="item-1", source_ref="src")
        job = DummyIngestionJob(base_config, items=[item], content_by_id={"item-1": "content"})
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.metadata_tracker.get_latest_record.return_value = Mock(checksum="rev-42", version=1)

        with (
            patch.object(job, "get_item_checksum", return_value="rev-42"),
            patch.object(job, "get_raw_content") as mock_fetch,
        ):
            result = job.process_item(item)

        assert result == 0
        mock_fetch.assert_not_called()
        job.vector_manager.insert_documents.assert_not_called()

    @patch("tasks.base.Document")
    def test_process_item_stores_when_pre_checksum_differs(self, mock_document, base_config):
        """Pre-checksum differs from DB → content fetched, stored checksum is pre-checksum."""
        content = "new content"
        last_modified = datetime(2024, 6, 1)
        item = IngestionItem(id="item-1", source_ref="src", last_modified=last_modified)
        job = DummyIngestionJob(base_config, items=[item], content_by_id={"item-1": content})
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.metadata_tracker.get_latest_record.return_value = Mock(checksum="rev-41", version=1)

        with (
            patch.object(job, "get_item_checksum", return_value="rev-42"),
            patch.object(job, "_seen_add", return_value=True),
        ):
            result = job.process_item(item)

        assert result == 1
        job.vector_manager.insert_documents.assert_called_once()
        job.metadata_tracker.record_metadata.assert_called_once_with(
            "item-1",
            "rev-42",  # stored checksum is the pre-computed one, not MD5
            2,
            1,
            last_modified,
            extra_metadata={"source_name": "test-source"},
        )
        _, kwargs = mock_document.call_args
        assert kwargs["metadata"]["checksum"] == "rev-42"

    def test_process_item_skips_when_seen_add_returns_false(self, base_config):
        """_seen_add returns False (duplicate checksum this run) → item skipped, content never fetched."""
        item = IngestionItem(id="item-1", source_ref="src")
        job = DummyIngestionJob(base_config, items=[item], content_by_id={"item-1": "content"})
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.metadata_tracker.get_latest_record.return_value = Mock(checksum="rev-old", version=1)

        with (
            patch.object(job, "get_item_checksum", return_value="rev-42"),
            patch.object(job, "_seen_add", return_value=False),
            patch.object(job, "get_raw_content") as mock_fetch,
        ):
            result = job.process_item(item)

        assert result == 0
        mock_fetch.assert_not_called()
        job.vector_manager.insert_documents.assert_not_called()

    def test_run_reports_totals(self, base_config):
        item1 = IngestionItem(id="item-1", source_ref="src")
        item2 = IngestionItem(id="item-2", source_ref="src")
        job = DummyIngestionJob(base_config, items=[item1, item2])
        job.process_item = Mock(side_effect=[1, 0])

        result = job.run()

        assert result == "[test-source] Completed: 1 ingested, 1 skipped"
        assert job.process_item.call_count == 2


class TestIngestionJobRunFatalErrors(unittest.TestCase):
    """run() must propagate fatal errors from list_items() instead of swallowing them.

    Written as unittest.TestCase (not the pytest-fixture style above) so it actually
    executes under `python -m unittest discover` per the project's test runner.
    """

    def test_run_reraises_list_items_exception(self):
        class FailingListItemsJob(IngestionJob):
            @property
            def source_type(self) -> str:
                return "dummy"

            def list_items(self):
                raise ConnectionError("auth failed")
                yield  # pragma: no cover — makes this a generator function

            def get_raw_content(self, item):
                return ""

            def get_item_name(self, item):
                return item.id

        job = FailingListItemsJob({"name": "test-source"})

        with self.assertRaises(ConnectionError):
            job.run()

    def test_run_does_not_swallow_fatal_error_into_return_string(self):
        """A fatal list_items() error must not be caught and turned into a result string —
        Celery's ignore_result=True means a returned string is silently discarded, so a
        caught-and-stringified error looks identical to success."""

        class FailingListItemsJob(IngestionJob):
            @property
            def source_type(self) -> str:
                return "dummy"

            def list_items(self):
                raise RuntimeError("listing API is down")
                yield  # pragma: no cover — makes this a generator function

            def get_raw_content(self, item):
                return ""

            def get_item_name(self, item):
                return item.id

        job = FailingListItemsJob({"name": "test-source"})

        try:
            job.run()
            self.fail("run() should have raised RuntimeError, not returned normally")
        except RuntimeError as exc:
            self.assertEqual(str(exc), "listing API is down")


class TestIngestionJobMarkdownConversion:
    def test_convert_bytes_returns_converted_text(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.return_value = Mock(markdown="# Hello")
        job._markitdown = mock_md

        result = job.convert_to_markdown(b"raw bytes")

        assert result == "# Hello"
        mock_md.convert_stream.assert_called_once()

    def test_convert_bytes_passes_file_extension_hint(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.return_value = Mock(markdown="# Hello")
        job._markitdown = mock_md

        job.convert_to_markdown(b"raw bytes", file_extension=".pdf")

        _, kwargs = mock_md.convert_stream.call_args
        assert kwargs["stream_info"].extension == ".pdf"

    def test_convert_bytes_without_file_extension_passes_no_stream_info(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.return_value = Mock(markdown="# Hello")
        job._markitdown = mock_md

        job.convert_to_markdown(b"raw bytes")

        _, kwargs = mock_md.convert_stream.call_args
        assert kwargs["stream_info"] is None

    def test_convert_bytes_falls_back_on_empty_result(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.return_value = Mock(markdown="   ")
        job._markitdown = mock_md

        result = job.convert_to_markdown(b"raw", fallback="raw text")

        assert result == "raw text"

    def test_convert_bytes_falls_back_on_exception(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.side_effect = RuntimeError("boom")
        job._markitdown = mock_md

        result = job.convert_to_markdown(b"raw", fallback="fallback")

        assert result == "fallback"

    def test_convert_bytes_default_fallback_is_empty_string(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.return_value = Mock(markdown="")
        job._markitdown = mock_md

        result = job.convert_to_markdown(b"raw")

        assert result == ""

    def test_convert_text_returns_converted_text(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.return_value = Mock(markdown="# Heading")
        job._markitdown = mock_md

        result = job.convert_to_markdown("some wiki text")

        assert result == "# Heading"

    def test_convert_text_falls_back_on_empty_result(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.return_value = Mock(markdown="   ")
        job._markitdown = mock_md

        result = job.convert_to_markdown("original text")

        assert result == "original text"

    def test_convert_text_falls_back_on_exception(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.side_effect = RuntimeError("oops")
        job._markitdown = mock_md

        result = job.convert_to_markdown("original text")

        assert result == "original text"

    def test_convert_text_returns_empty_string_unchanged(self):
        job = DummyIngestionJob({"name": "test-source"})
        assert job.convert_to_markdown("") == ""

    def test_convert_text_returns_whitespace_only_unchanged(self):
        job = DummyIngestionJob({"name": "test-source"})
        assert job.convert_to_markdown("   ") == "   "

    def test_get_markitdown_is_lazily_created(self):
        job = DummyIngestionJob({"name": "test-source"})
        assert job._markitdown is None
        with patch("tasks.base.MarkItDown") as mock_cls:
            instance = job._get_markitdown()
            mock_cls.assert_called_once_with()
            assert instance is mock_cls.return_value

    def test_get_markitdown_returns_same_instance(self):
        job = DummyIngestionJob({"name": "test-source"})
        with patch("tasks.base.MarkItDown"):
            first = job._get_markitdown()
            second = job._get_markitdown()
            assert first is second


class TestIngestionJobACL:
    def test_sanitize_acl_list_trims_lowercases_dedupes_sorts(self, base_config):
        job = DummyIngestionJob(base_config)
        result = job._sanitize_acl_list(
            [" Bob@Example.com ", "alice@example.com", "bob@example.com "],
            item_id="item-1",
        )
        assert result == ["alice@example.com", "bob@example.com"]

    def test_sanitize_acl_list_star_alone_passes_through(self, base_config):
        job = DummyIngestionJob(base_config)
        assert job._sanitize_acl_list(["*"], item_id="item-1") == ["*"]

    def test_sanitize_acl_list_mixed_star_and_emails_collapses_to_star(self, base_config, caplog):
        job = DummyIngestionJob(base_config)
        with caplog.at_level("WARNING"):
            result = job._sanitize_acl_list(["*", "bob@example.com"], item_id="item-1")
        assert result == ["*"]
        assert "mixes '*' with explicit" in caplog.text

    def test_sanitize_acl_list_empty_input_returns_empty(self, base_config):
        job = DummyIngestionJob(base_config)
        assert job._sanitize_acl_list([], item_id="item-1") == []

    def _acl_job(self, base_config, *, acl_owner=None, acl_return=None, acl_side_effect=None):
        config = {**base_config, "config": {"acl_owner": acl_owner} if acl_owner else {}}
        item = IngestionItem(id="item-1", source_ref="src")
        job = DummyIngestionJob(config, items=[item], content_by_id={"item-1": "content"})
        job.acl_enabled = True
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.metadata_tracker.get_latest_record.return_value = None
        if acl_side_effect is not None:
            job.get_acl_list = Mock(side_effect=acl_side_effect)
        else:
            job.get_acl_list = Mock(return_value=acl_return if acl_return is not None else [])
        return job, item

    @patch("tasks.base.Document")
    def test_acl_owner_fallback_applied_on_empty_acl(self, mock_document, base_config):
        job, item = self._acl_job(base_config, acl_owner="owner@example.com", acl_return=[])

        job.process_item(item)

        _, kwargs = mock_document.call_args
        assert kwargs["metadata"]["acl"] == ["owner@example.com"]

    @patch("tasks.base.Document")
    def test_acl_owner_fallback_not_applied_when_unset(self, mock_document, base_config):
        job, item = self._acl_job(base_config, acl_owner=None, acl_return=[])

        job.process_item(item)

        _, kwargs = mock_document.call_args
        assert kwargs["metadata"]["acl"] == []

    @patch("tasks.base.Document")
    def test_acl_owner_fallback_not_applied_on_get_acl_list_failure(self, mock_document, base_config):
        job, item = self._acl_job(base_config, acl_owner="owner@example.com", acl_side_effect=RuntimeError("boom"))

        result = job.process_item(item)

        assert result == 1
        _, kwargs = mock_document.call_args
        assert kwargs["metadata"]["acl"] == []

    @patch("tasks.base.Document")
    def test_get_acl_list_failure_does_not_crash_process_item(self, mock_document, base_config):
        job, item = self._acl_job(base_config, acl_side_effect=RuntimeError("boom"))

        result = job.process_item(item)

        assert result == 1
        job.vector_manager.insert_documents.assert_called_once()

    @patch("tasks.base.Document")
    def test_acl_disabled_skips_get_acl_list_and_omits_acl_key(self, mock_document, base_config):
        item = IngestionItem(id="item-1", source_ref="src")
        job = DummyIngestionJob(base_config, items=[item], content_by_id={"item-1": "content"})
        job.acl_enabled = False
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.metadata_tracker.get_latest_record.return_value = None
        job.get_acl_list = Mock(return_value=["bob@example.com"])

        job.process_item(item)

        job.get_acl_list.assert_not_called()
        _, kwargs = mock_document.call_args
        assert "acl" not in kwargs["metadata"]
        assert kwargs["excluded_llm_metadata_keys"] == []
        assert kwargs["excluded_embed_metadata_keys"] == []

    @patch("tasks.base.Document")
    def test_acl_enabled_stores_acl_and_excludes_from_llm_and_embed(self, mock_document, base_config):
        job, item = self._acl_job(base_config, acl_return=["bob@example.com", "alice@example.com"])

        job.process_item(item)

        _, kwargs = mock_document.call_args
        assert kwargs["metadata"]["acl"] == ["alice@example.com", "bob@example.com"]
        assert kwargs["excluded_llm_metadata_keys"] == ["acl"]
        assert kwargs["excluded_embed_metadata_keys"] == ["acl"]

    @patch("tasks.base.Document")
    def test_acl_only_change_triggers_reingestion_despite_same_checksum(self, mock_document, base_config):
        content = "same content"
        checksum = hashlib.md5(content.encode("utf-8"), usedforsecurity=False).hexdigest()
        item = IngestionItem(id="item-1", source_ref="src")
        job = DummyIngestionJob(base_config, items=[item], content_by_id={"item-1": content})
        job.acl_enabled = True
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.metadata_tracker.get_latest_record.return_value = Mock(
            checksum=checksum,
            version=1,
            metadata_content={"acl": ["alice@example.com"]},
        )
        job.get_acl_list = Mock(return_value=["bob@example.com"])

        result = job.process_item(item)

        assert result == 1
        job.metadata_tracker.delete_previous_embeddings.assert_called_once_with("item-1")
        job.vector_manager.insert_documents.assert_called_once()
        _, kwargs = mock_document.call_args
        assert kwargs["metadata"]["acl"] == ["bob@example.com"]

    def test_acl_and_content_unchanged_still_skipped(self, base_config):
        content = "same content"
        checksum = hashlib.md5(content.encode("utf-8"), usedforsecurity=False).hexdigest()
        item = IngestionItem(id="item-1", source_ref="src")
        job = DummyIngestionJob(base_config, items=[item], content_by_id={"item-1": content})
        job.acl_enabled = True
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.metadata_tracker.get_latest_record.return_value = Mock(
            checksum=checksum,
            version=1,
            metadata_content={"acl": ["alice@example.com"]},
        )
        job.get_acl_list = Mock(return_value=["alice@example.com"])

        with patch.object(job, "_seen_add", return_value=True):
            result = job.process_item(item)

        assert result == 0
        job.metadata_tracker.delete_previous_embeddings.assert_not_called()
        job.vector_manager.insert_documents.assert_not_called()

    @patch("tasks.base.Document")
    def test_get_extra_metadata_acl_key_is_filtered_out(self, mock_document, base_config):
        job, item = self._acl_job(base_config, acl_return=["alice@example.com"])
        job.get_extra_metadata = Mock(return_value={"acl": ["attacker@example.com"], "custom": "val"})

        job.process_item(item)

        _, kwargs = mock_document.call_args
        assert kwargs["metadata"]["acl"] == ["alice@example.com"]
        assert kwargs["metadata"]["custom"] == "val"
