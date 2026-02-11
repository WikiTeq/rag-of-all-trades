import hashlib
import pytest
from datetime import datetime
from unittest.mock import Mock, patch

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem


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
        for item in self._items:
            yield item

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
        from tasks.base import RESERVED_METADATA_KEYS

        job = DummyIngestionJob(base_config)
        job.get_extra_metadata = Mock(
            return_value={k: "overwrite" for k in RESERVED_METADATA_KEYS}
        )

        item = IngestionItem(id="item-1", source_ref="src")
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.get_raw_content = Mock(return_value="content")
        job.metadata_tracker.get_latest_record.return_value = None

        job.process_item(item)

        args, _ = job.vector_manager.insert_documents.call_args
        metadata = args[0][0].metadata
        for key in RESERVED_METADATA_KEYS:
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
        checksum = hashlib.md5(content.encode("utf-8")).hexdigest()
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
        checksum = hashlib.md5(content.encode("utf-8")).hexdigest()
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

    def test_run_reports_totals(self, base_config):
        item1 = IngestionItem(id="item-1", source_ref="src")
        item2 = IngestionItem(id="item-2", source_ref="src")
        job = DummyIngestionJob(base_config, items=[item1, item2])
        job.process_item = Mock(side_effect=[1, 0])

        result = job.run()

        assert result == "[test-source] Completed: 1 ingested, 1 skipped"
        assert job.process_item.call_count == 2
