import hashlib
import unittest
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


class TestIngestionJob(unittest.TestCase):
    def setUp(self):
        self.config = {"name": "test-source"}

    def test_get_document_metadata_defaults(self):
        job = DummyIngestionJob(self.config)
        item = IngestionItem(
            id="item-1",
            source_ref="src",
            last_modified=datetime(2024, 1, 1),
        )

        metadata = job.get_document_metadata(
            item=item,
            item_name="item-1",
            checksum="abc",
            version=2,
            last_modified=item.last_modified,
        )

        self.assertEqual(metadata["source"], "dummy")
        self.assertEqual(metadata["key"], "item-1")
        self.assertEqual(metadata["checksum"], "abc")
        self.assertEqual(metadata["version"], 2)
        self.assertEqual(metadata["format"], "markdown")
        self.assertEqual(metadata["source_name"], "test-source")
        self.assertEqual(metadata["file_name"], "item-1")
        self.assertEqual(metadata["last_modified"], "2024-01-01 00:00:00")

    def test_seen_add_lru_eviction(self):
        job = DummyIngestionJob(self.config)
        job._seen_capacity = 2

        self.assertTrue(job._seen_add("a"))
        self.assertTrue(job._seen_add("b"))
        self.assertFalse(job._seen_add("a"))
        self.assertTrue(job._seen_add("c"))
        self.assertTrue(job._seen_add("b"))

    def test_process_item_skips_empty_content(self):
        item = IngestionItem(id="item-1", source_ref="src")
        job = DummyIngestionJob(
            self.config,
            items=[item],
            content_by_id={"item-1": "   "},
        )
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()

        result = job.process_item(item)

        self.assertEqual(result, 0)
        job.metadata_tracker.get_latest_record.assert_not_called()
        job.vector_manager.insert_documents.assert_not_called()

    def test_process_item_skips_unchanged_content(self):
        content = "same content"
        checksum = hashlib.md5(content.encode("utf-8")).hexdigest()
        item = IngestionItem(id="item-1", source_ref="src")
        job = DummyIngestionJob(
            self.config,
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

        self.assertEqual(result, 0)
        job.metadata_tracker.get_latest_record.assert_called_once_with("item-1")
        job.metadata_tracker.delete_previous_embeddings.assert_not_called()
        job.metadata_tracker.record_metadata.assert_not_called()
        job.vector_manager.insert_documents.assert_not_called()

    @patch("tasks.base.Document")
    def test_process_item_updates_version_and_records_metadata(self, mock_document):
        content = "updated content"
        checksum = hashlib.md5(content.encode("utf-8")).hexdigest()
        last_modified = datetime(2024, 1, 2, 3, 4, 5)
        item = IngestionItem(
            id="item-1",
            source_ref="src",
            last_modified=last_modified,
        )
        job = DummyIngestionJob(
            self.config,
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

        self.assertEqual(result, 1)
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

        self.assertEqual(mock_document.call_count, 1)
        _, kwargs = mock_document.call_args
        self.assertEqual(kwargs["text"], content)
        self.assertEqual(kwargs["metadata"]["checksum"], checksum)
        self.assertEqual(kwargs["metadata"]["version"], 3)
        self.assertEqual(kwargs["metadata"]["source"], "dummy")

    def test_run_reports_totals(self):
        item1 = IngestionItem(id="item-1", source_ref="src")
        item2 = IngestionItem(id="item-2", source_ref="src")
        job = DummyIngestionJob(self.config, items=[item1, item2])
        job.process_item = Mock(side_effect=[1, 0])

        result = job.run()

        self.assertEqual(result, "[test-source] Completed: 1 ingested, 1 skipped")
        self.assertEqual(job.process_item.call_count, 2)


if __name__ == "__main__":
    unittest.main()
