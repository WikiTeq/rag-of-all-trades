import unittest
from unittest.mock import patch

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.pipedrive_ingestion import PipedriveIngestionJob


def _make_config(**overrides):
    cfg = {
        "api_token": "test-token",
        "load_types": ["deals", "notes"],
        "max_items": 10,
        "request_delay": 0,
        "max_retries": 0,
    }
    cfg.update(overrides)
    return {"name": "test_pipedrive", "config": cfg}


def _make_job(config=None, **overrides):
    if config is None:
        config = _make_config(**overrides)
    with patch("tasks.pipedrive_ingestion.PipedriveClient") as MockClient:
        instance = MockClient.return_value
        instance.paginate.return_value = iter([])
        instance.get.return_value = {"success": True, "data": []}
        instance.resolve_user.return_value = "Unknown User"
        instance.resolve_pipeline.return_value = "Unknown Pipeline"
        instance.resolve_stage.return_value = "Unknown Stage"
        job = PipedriveIngestionJob(config)
        job._client = instance
    return job


class TestPipedriveIngestionInit(unittest.TestCase):
    def test_init_success(self):
        job = _make_job()
        self.assertEqual(job.source_type, "pipedrive")
        self.assertEqual(job.api_token, "test-token")
        self.assertIn("deals", job.load_types)

    def test_init_missing_api_token(self):
        with self.assertRaises(ValueError):
            _make_job(api_token="")

    def test_init_unknown_load_type(self):
        with self.assertRaises(ValueError):
            _make_job(load_types=["nonexistent_entity"])

    def test_default_load_types_all(self):
        job = _make_job(load_types=None)
        self.assertGreater(len(job.load_types), 5)


class TestPipedriveListItems(unittest.TestCase):
    def test_list_items_yields_ingestion_items(self):
        job = _make_job(load_types=["deals"])
        deal = {
            "id": 42,
            "title": "Big Deal",
            "update_time": "2024-01-15 10:00:00",
            "stage_id": 1,
            "pipeline_id": 1,
        }
        job._client.paginate.return_value = iter([deal])

        items = list(job.list_items())
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertIsInstance(item, IngestionItem)
        self.assertEqual(item.id, "pipedrive:deals:42")

    def test_list_items_multi_entity(self):
        job = _make_job(load_types=["deals", "notes"])
        job._client.paginate.side_effect = [
            iter([{"id": 1, "title": "Deal A", "update_time": None}]),
            iter([{"id": 2, "content": "Note text", "update_time": None}]),
        ]
        items = list(job.list_items())
        ids = [i.id for i in items]
        self.assertIn("pipedrive:deals:1", ids)
        self.assertIn("pipedrive:notes:2", ids)

    def test_list_items_respects_max_items(self):
        job = _make_job(load_types=["deals"], max_items=2)
        deals = [{"id": i, "title": f"Deal {i}", "update_time": None} for i in range(10)]
        job._client.paginate.return_value = iter(deals)

        items = list(job.list_items())
        self.assertLessEqual(len(items), 2)


class TestPipedriveGetRawContent(unittest.TestCase):
    def _item(self, entity_type, record):
        return IngestionItem(
            id=f"pipedrive:{entity_type}:{record['id']}",
            source_ref={"type": entity_type, "data": record},
            last_modified=None,
        )

    def test_deal_content_contains_title(self):
        job = _make_job(load_types=["deals"])
        record = {
            "id": 1,
            "title": "Mega Deal",
            "status": "open",
            "value": 5000,
            "currency": "USD",
            "stage_id": 1,
            "pipeline_id": 1,
            "person_name": "Alice",
            "org_name": "Acme",
        }
        item = self._item("deals", record)
        content = job.get_raw_content(item)
        self.assertIn("Mega Deal", content)

    def test_note_content_contains_note_text(self):
        job = _make_job(load_types=["notes"])
        record = {
            "id": 5,
            "content": "Important note about the client.",
            "deal_title": "Some Deal",
        }
        job._client.get.return_value = {"success": True, "data": []}
        item = self._item("notes", record)
        content = job.get_raw_content(item)
        self.assertIn("Important note about the client.", content)

    def test_unknown_entity_falls_back_to_generic(self):
        job = _make_job(load_types=["leads"])
        record = {"id": 9, "title": "Lead title", "owner_name": "Bob"}
        item = self._item("leads", record)
        content = job.get_raw_content(item)
        self.assertIsInstance(content, str)
        self.assertGreater(len(content), 0)


class TestPipedriveGetItemName(unittest.TestCase):
    def test_item_name_format(self):
        job = _make_job()
        item = IngestionItem(
            id="pipedrive:deals:99",
            source_ref={"type": "deals", "data": {"id": 99, "title": "Test Deal"}},
            last_modified=None,
        )
        name = job.get_item_name(item)
        self.assertIn("deals", name)
        self.assertIn("99", name)
        self.assertLessEqual(len(name), 255)


class TestPipedriveGetDocumentMetadata(unittest.TestCase):
    def test_metadata_includes_pipedrive_id(self):
        job = _make_job()
        item = IngestionItem(
            id="pipedrive:deals:1",
            source_ref={"type": "deals", "data": {"id": 1, "title": "Deal"}},
            last_modified=None,
        )
        meta = job.get_extra_metadata(item, "", {})
        self.assertEqual(meta.get("pipedrive_id"), "1")

    def test_metadata_includes_entity_type(self):
        job = _make_job()
        item = IngestionItem(
            id="pipedrive:persons:7",
            source_ref={"type": "persons", "data": {"id": 7, "name": "Alice"}},
            last_modified=None,
        )
        meta = job.get_extra_metadata(item, "", {})
        self.assertEqual(meta.get("entity_type"), "persons")


if __name__ == "__main__":
    unittest.main()
