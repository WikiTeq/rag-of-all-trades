import unittest
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from asana.rest import ApiException

from tasks.asana_ingestion import AsanaIngestionJob
from tasks.factory import IngestionJobFactory
from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.schemas import BaseMetadataSchema


def _make_config(name="asana1", **overrides):
    cfg = {"asana_token": "tok", "workspace_id": "111"}
    cfg.update(overrides)
    return {"name": name, "config": cfg}


def _make_task(
    gid="1200000000000001",
    name="Fix login",
    notes="Users cannot log in.",
    **overrides,
):
    task = {
        "gid": gid,
        "name": name,
        "notes": notes,
        "completed": False,
        "permalink_url": f"https://app.asana.com/0/222/{gid}",
        "modified_at": "2026-05-01T10:00:00.000Z",
        "assignee": {"gid": "9", "name": "Ada"},
        "followers": [{"gid": "10", "name": "Bob"}, {"gid": "11", "name": "Cy"}],
        "custom_fields": [
            {"name": "Priority", "display_value": "High"},
            {"name": "Estimate", "display_value": None},
        ],
        "memberships": [{"project": {"gid": "222", "name": "Web"}}],
        "created_at": "2026-04-01T09:00:00.000Z",
        "due_on": "2026-05-15",
        "tags": [{"gid": "5", "name": "bug"}, {"gid": "6", "name": "urgent"}],
    }
    task.update(overrides)
    return task


def _make_job(config=None, **overrides):
    """Create an AsanaIngestionJob with the Asana SDK patched out."""
    with patch("tasks.asana_ingestion.asana"):
        job = AsanaIngestionJob(config or _make_config(**overrides))
    return job


def _item(task):
    return IngestionItem(id=f"asana:asana1:task:{task['gid']}", source_ref=task)


def _collect_one(job, task):
    """Run list_items() for a single task in workspace mode and return the item."""
    job._projects_api.get_projects.return_value = iter([{"gid": "p1"}])
    job._tasks_api.get_tasks_for_project.return_value = iter([task])
    (item,) = list(job.list_items())
    return item


class TestAsanaInit(unittest.TestCase):
    def test_init_workspace_mode(self):
        job = _make_job()
        self.assertEqual(job.source_type, "asana")
        self.assertEqual(job.workspace_id, "111")
        self.assertEqual(job.project_id, "")

    def test_init_builds_sdk_client_once_with_token(self):
        with patch("tasks.asana_ingestion.asana") as sdk:
            job = AsanaIngestionJob(_make_config())
        self.assertEqual(sdk.Configuration.return_value.access_token, "tok")
        sdk.ApiClient.assert_called_once_with(sdk.Configuration.return_value)
        self.assertIs(job._tasks_api, sdk.TasksApi.return_value)
        self.assertIs(job._projects_api, sdk.ProjectsApi.return_value)
        self.assertIs(job._stories_api, sdk.StoriesApi.return_value)

    def test_init_accepts_integer_ids(self):
        job = _make_job(workspace_id=111, project_id=222)
        self.assertEqual(job.workspace_id, "111")
        self.assertEqual(job.project_id, "222")

    def test_init_with_real_sdk_makes_no_request(self):
        job = AsanaIngestionJob(_make_config())
        self.assertEqual(job.workspace_id, "111")

    def test_missing_token_raises(self):
        for token in ("", None, "   "):
            with self.subTest(token=token), self.assertRaisesRegex(ValueError, "asana_token is required"):
                _make_job(asana_token=token)

    def test_missing_workspace_raises(self):
        for ws in ("", None, "  "):
            with self.subTest(ws=ws), self.assertRaisesRegex(ValueError, "workspace_id is required"):
                _make_job(workspace_id=ws)

    def test_non_numeric_ids_raise(self):
        with self.assertRaisesRegex(ValueError, "workspace_id must be a numeric"):
            _make_job(workspace_id="my-workspace")
        with self.assertRaisesRegex(ValueError, "project_id must be a numeric"):
            _make_job(project_id="abc")

    def test_comment_options_defaults_and_overrides(self):
        job = _make_job()
        self.assertTrue(job.load_comments)
        self.assertEqual(job.max_comments, 10)
        job = _make_job(load_comments="false", max_comments="5")
        self.assertFalse(job.load_comments)
        self.assertEqual(job.max_comments, 5)

    def test_non_positive_max_comments_raises(self):
        for value in (0, -1):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "max_comments must be positive"):
                _make_job(max_comments=value)

    def test_blank_project_id_raises_instead_of_widening_to_workspace(self):
        for pid in ("", None, "  "):
            with self.subTest(pid=pid), self.assertRaisesRegex(ValueError, "project_id is required"):
                _make_job(project_id=pid)


class TestAsanaListItems(unittest.TestCase):
    def test_workspace_mode_iterates_projects(self):
        job = _make_job()
        job._projects_api.get_projects.return_value = iter([{"gid": "p1"}, {"gid": "p2"}])
        job._tasks_api.get_tasks_for_project.side_effect = [
            iter([_make_task("1")]),
            iter([_make_task("2")]),
        ]
        items = list(job.list_items())
        self.assertEqual([i.id for i in items], ["asana:asana1:task:1", "asana:asana1:task:2"])
        job._projects_api.get_projects.assert_called_once_with(
            {"workspace": "111", "archived": False, "opt_fields": "gid"}
        )
        self.assertEqual(
            [c.args[0] for c in job._tasks_api.get_tasks_for_project.call_args_list],
            ["p1", "p2"],
        )
        job._projects_api.get_project.assert_not_called()

    def test_project_mode_reads_only_that_project(self):
        job = _make_job(project_id="222")
        job._projects_api.get_project.return_value = {"gid": "222", "workspace": {"gid": "111"}}
        job._tasks_api.get_tasks_for_project.return_value = iter([_make_task("1")])
        items = list(job.list_items())
        self.assertEqual(len(items), 1)
        job._projects_api.get_projects.assert_not_called()
        self.assertEqual(job._tasks_api.get_tasks_for_project.call_args.args[0], "222")

    def test_project_in_other_workspace_raises(self):
        job = _make_job(project_id="222")
        job._projects_api.get_project.return_value = {"gid": "222", "workspace": {"gid": "999"}}
        with self.assertRaisesRegex(ValueError, "belongs to workspace '999'"):
            list(job.list_items())
        job._tasks_api.get_tasks_for_project.assert_not_called()

    def test_item_ids_differ_between_sources_for_the_same_task(self):
        ids = []
        for name in ("workspace-src", "project-src"):
            job = _make_job(_make_config(name=name))
            ids.append(_collect_one(job, _make_task("42")).id)
        self.assertEqual(ids, ["asana:workspace-src:task:42", "asana:project-src:task:42"])

    def test_shared_task_is_yielded_once(self):
        job = _make_job()
        job._projects_api.get_projects.return_value = iter([{"gid": "p1"}, {"gid": "p2"}])
        job._tasks_api.get_tasks_for_project.side_effect = [
            iter([_make_task("1"), _make_task("2")]),
            iter([_make_task("1"), _make_task("3")]),
        ]
        ids = [i.id for i in job.list_items()]
        self.assertEqual(ids, ["asana:asana1:task:1", "asana:asana1:task:2", "asana:asana1:task:3"])
        job._stories_api.get_stories_for_task.assert_not_called()

    def test_empty_workspace_yields_nothing(self):
        job = _make_job()
        job._projects_api.get_projects.return_value = iter([])
        self.assertEqual(list(job.list_items()), [])

    def test_last_modified_is_parsed_datetime(self):
        job = _make_job()
        job._projects_api.get_projects.return_value = iter([{"gid": "p1"}])
        job._tasks_api.get_tasks_for_project.return_value = iter([_make_task("1"), _make_task("2", modified_at=None)])
        first, second = list(job.list_items())
        self.assertEqual(first.last_modified, datetime(2026, 5, 1, 10, 0, tzinfo=UTC))
        self.assertIsNone(second.last_modified)

    def test_task_without_gid_raises(self):
        job = _make_job()
        job._projects_api.get_projects.return_value = iter([{"gid": "p1"}])
        job._tasks_api.get_tasks_for_project.return_value = iter([{"name": "no id"}])
        with self.assertRaisesRegex(ValueError, "without gid"):
            list(job.list_items())

    def test_project_listing_error_propagates(self):
        job = _make_job()
        job._projects_api.get_projects.side_effect = ApiException(status=401, reason="Unauthorized")
        with self.assertRaises(ApiException):
            list(job.list_items())

    def test_task_listing_error_propagates(self):
        job = _make_job()
        job._projects_api.get_projects.return_value = iter([{"gid": "p1"}])
        job._tasks_api.get_tasks_for_project.side_effect = ApiException(status=500, reason="boom")
        with self.assertRaises(ApiException):
            list(job.list_items())


class TestAsanaGetRawContent(unittest.TestCase):
    def test_title_notes_and_comments(self):
        job = _make_job()
        stories = [
            {"text": "Looks bad", "created_at": "2026-05-02T09:00:00Z", "created_by": {"name": "Bob"}},
            {"text": "  ", "created_by": {"name": "Skip"}},
            {"text": "Fixed", "created_at": "2026-05-03T09:00:00Z", "created_by": None},
        ]
        job._stories_api.get_stories_for_task.return_value = iter(stories)
        item = _collect_one(job, _make_task())
        self.assertEqual(
            job.get_raw_content(item),
            "# Fix login\n\nUsers cannot log in.\n\n## Comments\n\n"
            "**Bob** (2026-05-02T09:00:00Z):\nLooks bad\n\n"
            "**Unknown** (2026-05-03T09:00:00Z):\nFixed",
        )
        job._stories_api.get_stories_for_task.assert_called_once_with(
            "1200000000000001",
            {
                "resource_subtype": "comment_added",
                "sort_ascending": False,
                "opt_fields": "text,created_at,created_by.name",
            },
            item_limit=10,
        )

    def test_missing_notes_and_no_comments(self):
        job = _make_job()
        item = _collect_one(job, _make_task(notes=None))
        self.assertEqual(job.get_raw_content(item), "# Fix login")

    def test_comments_are_limited_to_max_comments(self):
        job = _make_job(max_comments=3)
        job.get_raw_content(_item(_make_task()))
        self.assertEqual(job._stories_api.get_stories_for_task.call_args.kwargs, {"item_limit": 3})

    def test_load_comments_false_skips_comment_requests(self):
        job = _make_job(load_comments="false")
        item = _collect_one(job, _make_task())
        self.assertEqual(job.get_raw_content(item), "# Fix login\n\nUsers cannot log in.")
        job._stories_api.get_stories_for_task.assert_not_called()


class TestAsanaGetItemName(unittest.TestCase):
    @staticmethod
    def _name(source, gid="42"):
        job = _make_job(_make_config(name=source))
        item = IngestionItem(id=f"asana:{source}:task:{gid}", source_ref=_make_task(gid))
        return job.get_item_name(item)

    def test_name_is_source_hash_then_gid(self):
        self.assertRegex(self._name("asana1"), r"^asana_[0-9a-f]{32}_42$")

    def test_same_title_different_tasks_differ(self):
        job = _make_job()
        a = job.get_item_name(_item(_make_task("1", name="Same")))
        b = job.get_item_name(_item(_make_task("2", name="Same")))
        self.assertNotEqual(a, b)

    def test_stable_across_instances(self):
        self.assertEqual(self._name("asana1", "7"), self._name("asana1", "7"))

    def test_two_sources_get_distinct_names(self):
        self.assertNotEqual(self._name("proj-a"), self._name("proj-b"))

    def test_sources_with_same_sanitized_text_differ(self):
        self.assertNotEqual(self._name("team a"), self._name("team_a"))

    def test_long_source_names_stay_short_and_differ(self):
        a, b = self._name("s" * 300 + "a", "1200000000000001"), self._name("s" * 300 + "b", "1200000000000001")
        self.assertLessEqual(len(a), 255)
        self.assertTrue(a.endswith("_1200000000000001"))
        self.assertNotEqual(a, b)


class TestAsanaGetExtraMetadata(unittest.TestCase):
    def test_full_metadata(self):
        job = _make_job()
        meta = job.get_extra_metadata(_item(_make_task()), "", {})
        self.assertEqual(
            meta,
            {
                "task_id": "1200000000000001",
                "task_title": "Fix login",
                "url": "https://app.asana.com/0/222/1200000000000001",
                "completed": False,
                "created_at": "2026-04-01T09:00:00.000Z",
                "due_on": "2026-05-15",
                "tags": ["bug", "urgent"],
                "workspace_id": "111",
                "assignee": "Ada",
                "followers": ["Bob", "Cy"],
                "projects": ["Web"],
                "custom_fields": ["Priority: High"],
            },
        )

    def test_same_named_custom_fields_are_both_kept(self):
        job = _make_job()
        task = _make_task(
            custom_fields=[
                {"gid": "1", "name": "Priority", "display_value": "High"},
                {"gid": "2", "name": "Priority", "display_value": "Low"},
            ]
        )
        meta = job.get_extra_metadata(_item(task), "", {})
        self.assertEqual(meta["custom_fields"], ["Priority: High", "Priority: Low"])

    def test_missing_and_null_fields_are_omitted(self):
        job = _make_job()
        task = _make_task(
            assignee=None,
            followers=None,
            custom_fields=None,
            memberships=None,
            permalink_url=None,
            created_at=None,
            due_on=None,
            tags=None,
        )
        meta = job.get_extra_metadata(_item(task), "", {})
        self.assertEqual(
            meta,
            {"task_id": "1200000000000001", "task_title": "Fix login", "completed": False, "workspace_id": "111"},
        )

    def test_no_reserved_key_collisions(self):
        job = _make_job()
        meta = job.get_extra_metadata(_item(_make_task()), "", {})
        self.assertFalse(set(meta) & set(BaseMetadataSchema.model_fields))


class TestAsanaChangeDetection(unittest.TestCase):
    def _checksum(self, task=None, **job_overrides):
        return _make_job(**job_overrides).get_item_checksum(_item(task or _make_task()))

    def test_stable_for_identical_input(self):
        self.assertEqual(self._checksum(), self._checksum())

    def test_new_modified_at_changes_checksum(self):
        self.assertNotEqual(self._checksum(), self._checksum(_make_task(modified_at="2027-01-01T00:00:00Z")))

    def test_checksum_loads_comments_once_per_task(self):
        job = _make_job()
        job.get_item_checksum(_item(_make_task()))
        job._stories_api.get_stories_for_task.assert_called_once()

    def test_checksum_makes_no_comment_request_when_comments_are_off(self):
        job = _make_job(load_comments=False)
        job.get_item_checksum(_item(_make_task()))
        job._stories_api.get_stories_for_task.assert_not_called()

    def test_edited_comment_changes_checksum(self):
        def checksum(text):
            job = _make_job()
            job._stories_api.get_stories_for_task.return_value = iter(
                [{"text": text, "created_at": "x", "created_by": {"name": "Bob"}}]
            )
            return job.get_item_checksum(_item(_make_task()))

        self.assertEqual(checksum("old"), checksum("old"))
        self.assertNotEqual(checksum("old"), checksum("new"))

    def test_comment_options_change_checksum(self):
        base = self._checksum()
        self.assertNotEqual(base, self._checksum(load_comments=False))
        self.assertNotEqual(base, self._checksum(max_comments=5))

    def test_metadata_only_changes_change_checksum(self):
        base = self._checksum()
        changes = {
            "assignee": {"gid": "9", "name": "Eve"},
            "followers": [{"gid": "10", "name": "Bob"}],
            "completed": True,
            "custom_fields": [{"name": "Priority", "display_value": "Low"}],
        }
        for field, value in changes.items():
            with self.subTest(field=field):
                self.assertNotEqual(base, self._checksum(_make_task(**{field: value})))


class TestAsanaProcessItem(unittest.TestCase):
    def test_ingests_document_with_metadata_and_skips_unchanged_and_reingests_on_change(self):
        job = _make_job()
        job.metadata_tracker = MagicMock()
        job.vector_manager = MagicMock()
        job.metadata_tracker.get_latest_record.return_value = None
        task = _make_task()
        item = _item(task)
        job._stories_api.get_stories_for_task.side_effect = lambda *args, **kwargs: iter(
            [{"text": "hi", "created_at": "x", "created_by": {"name": "Bob"}}]
        )

        self.assertEqual(job.process_item(item), 1)

        (doc,) = job.vector_manager.insert_documents.call_args.args[0]
        self.assertEqual(doc.metadata["key"], job.get_item_name(item))
        self.assertEqual(doc.metadata["checksum"], job.get_item_checksum(item))
        self.assertEqual(doc.metadata["task_id"], "1200000000000001")
        self.assertEqual(doc.metadata["custom_fields"], ["Priority: High"])
        self.assertIn("## Comments", doc.text)

        latest = MagicMock(checksum=job.get_item_checksum(item), version=1)
        job.metadata_tracker.get_latest_record.return_value = latest
        job._seen.clear()
        job._stories_api.get_stories_for_task.reset_mock()
        self.assertEqual(job.process_item(item), 0)
        job._stories_api.get_stories_for_task.assert_called_once()

        job._stories_api.get_stories_for_task.side_effect = lambda *args, **kwargs: iter([])
        changed = _item(_make_task(modified_at="2026-06-01T10:00:00.000Z"))
        self.assertEqual(job.process_item(changed), 1)
        job.metadata_tracker.delete_previous_embeddings.assert_called_once_with(job.get_item_name(item))

    def test_failed_comments_request_stores_nothing_and_keeps_old_embeddings(self):
        job = _make_job()
        job.metadata_tracker = MagicMock()
        job.vector_manager = MagicMock()
        job.metadata_tracker.get_latest_record.return_value = MagicMock(checksum="old", version=1)
        job._stories_api.get_stories_for_task.side_effect = ApiException(status=429, reason="rate limited")

        self.assertEqual(job.process_item(_item(_make_task())), 0)

        job.vector_manager.insert_documents.assert_not_called()
        job.metadata_tracker.delete_previous_embeddings.assert_not_called()
        job.metadata_tracker.record_metadata.assert_not_called()


class TestAsanaRun(unittest.TestCase):
    def _job(self):
        job = _make_job()
        job.metadata_tracker = MagicMock()
        job.vector_manager = MagicMock()
        job.metadata_tracker.get_latest_record.return_value = None
        job.request_delay = 0
        return job

    def test_failed_comments_request_is_counted_as_failed_not_skipped(self):
        job = self._job()
        job._projects_api.get_projects.return_value = iter([{"gid": "p1"}])
        job._tasks_api.get_tasks_for_project.return_value = iter([_make_task()])
        job._stories_api.get_stories_for_task.side_effect = ApiException(status=429, reason="rate limited")
        self.assertEqual(job.run(), "[asana1] Completed: 0 ingested, 0 skipped, 1 failed")

    def test_unchanged_task_is_counted_as_skipped(self):
        job = self._job()
        task = _make_task()
        job._stories_api.get_stories_for_task.return_value = iter([])
        job.metadata_tracker.get_latest_record.return_value = MagicMock(
            checksum=job.get_item_checksum(_item(task)), version=1
        )
        job._projects_api.get_projects.return_value = iter([{"gid": "p1"}])
        job._tasks_api.get_tasks_for_project.return_value = iter([task])
        job._stories_api.get_stories_for_task.return_value = iter([])
        self.assertEqual(job.run(), "[asana1] Completed: 0 ingested, 1 skipped, 0 failed")

    def test_failure_does_not_stop_later_tasks(self):
        job = self._job()
        job._projects_api.get_projects.return_value = iter([{"gid": "p1"}])
        job._tasks_api.get_tasks_for_project.return_value = iter([_make_task(gid="1"), _make_task(gid="2")])
        job._stories_api.get_stories_for_task.side_effect = [ApiException(status=500, reason="boom"), iter([])]
        self.assertEqual(job.run(), "[asana1] Completed: 1 ingested, 0 skipped, 1 failed")


class TestAsanaFactory(unittest.TestCase):
    def test_factory_creates_asana_job(self):
        with patch("tasks.asana_ingestion.asana"):
            job = IngestionJobFactory.create("asana", _make_config())
        self.assertIsInstance(job, AsanaIngestionJob)


if __name__ == "__main__":
    unittest.main()
