import unittest
from unittest.mock import Mock, patch

import requests

from tasks.clickup_ingestion import ClickUpIngestionJob
from tasks.factory import IngestionJobFactory
from tasks.helper_classes.ingestion_item import IngestionItem


def _make_config(**overrides):
    cfg = {"api_token": "test-token", "team_id": "1234567"}
    cfg.update(overrides)
    return {"name": "test_clickup", "config": cfg}


def _make_job(**overrides):
    with patch("tasks.clickup_ingestion.RetrySession") as session_cls:
        job = ClickUpIngestionJob(_make_config(**overrides))
    job._session = session_cls.return_value
    return job


def _response(payload, status=200):
    resp = Mock()
    resp.status_code = status
    resp.json.return_value = payload
    resp.raise_for_status.side_effect = None if status < 400 else requests.HTTPError(f"{status}")
    return resp


def _make_task(task_id="t1", **overrides):
    task = {
        "id": task_id,
        "name": "Fix login",
        "url": f"https://app.clickup.com/t/{task_id}",
        "description": "plain description",
        "markdown_description": "**markdown** description",
        "status": {"status": "in progress"},
        "list": {"name": "Sprint"},
        "project": {"name": "Backend"},
        "folder": {"name": "Engineering"},
        "space": {"id": "90"},
        "tags": [{"name": "bug"}, {"name": "urgent"}],
        "priority": {"priority": "high"},
        "creator": {"username": "alice", "email": "alice@example.com"},
        "assignees": [{"username": "bob", "email": "bob@example.com"}],
        "date_created": "1700000000000",
        "date_updated": "1700003600000",
        "date_closed": None,
        "due_date": "1700100000000",
    }
    task.update(overrides)
    return task


def _make_comment(comment_id, text="hello", date="1700000100000"):
    return {"id": comment_id, "comment_text": text, "date": date, "user": {"username": "carol"}}


def _item(task=None):
    task = task or _make_task()
    return IngestionItem(id=f"clickup:task:{task['id']}", source_ref=task)


class TestClickUpConfig(unittest.TestCase):
    def test_defaults(self):
        job = _make_job()
        self.assertEqual(job.source_type, "clickup")
        self.assertEqual(job.connector_type, "workspace")
        self.assertEqual(job.connector_ids, [])
        self.assertTrue(job.retrieve_task_comments)
        self.assertEqual(job.max_requests_per_minute, 100)

    def test_registered_in_factory(self):
        with patch("tasks.clickup_ingestion.RetrySession"):
            job = IngestionJobFactory.create("clickup", _make_config())
        self.assertIsInstance(job, ClickUpIngestionJob)

    def test_missing_credentials_rejected(self):
        with self.assertRaisesRegex(ValueError, "api_token"):
            _make_job(api_token="")
        with self.assertRaisesRegex(ValueError, "team_id"):
            _make_job(team_id="")

    def test_invalid_connector_type_rejected(self):
        with self.assertRaisesRegex(ValueError, "connector_type"):
            _make_job(connector_type="team")

    def test_scoped_type_requires_ids(self):
        with self.assertRaisesRegex(ValueError, "connector_ids"):
            _make_job(connector_type="space")

    def test_invalid_numbers_rejected(self):
        with self.assertRaises(ValueError):
            _make_job(max_retries=-1)
        with self.assertRaises(ValueError):
            _make_job(max_requests_per_minute=0)

    def test_string_booleans_and_comma_ids(self):
        job = _make_job(retrieve_task_comments="false", connector_type="list", connector_ids="1, 2")
        self.assertFalse(job.retrieve_task_comments)
        self.assertEqual(job.connector_ids, ["1", "2"])


class TestClickUpListItems(unittest.TestCase):
    def test_workspace_scope_has_no_filter_and_sets_auth(self):
        job = _make_job()
        job._session.get.return_value = _response({"tasks": [_make_task()]})

        items = list(job.list_items())

        self.assertEqual([i.id for i in items], ["clickup:task:t1"])
        args, kwargs = job._session.get.call_args
        self.assertEqual(args[0], "https://api.clickup.com/api/v2/team/1234567/task")
        self.assertEqual(kwargs["headers"], {"Authorization": "test-token"})
        self.assertEqual(kwargs["params"]["include_markdown_description"], "true")
        self.assertFalse(any(key.endswith("_ids[]") for key in kwargs["params"]))

    def test_scope_filters(self):
        for connector_type, key in (("space", "space_ids[]"), ("folder", "project_ids[]"), ("list", "list_ids[]")):
            job = _make_job(connector_type=connector_type, connector_ids="10,20")
            job._session.get.return_value = _response({"tasks": []})
            list(job.list_items())
            self.assertEqual(job._session.get.call_args.kwargs["params"][key], ["10", "20"])

    def test_pagination_stops_on_short_page(self):
        job = _make_job()
        full_page = [_make_task(str(n)) for n in range(100)]
        job._session.get.side_effect = [_response({"tasks": full_page}), _response({"tasks": [_make_task("x")]})]

        items = list(job.list_items())

        self.assertEqual(len(items), 101)
        pages = [call.kwargs["params"]["page"] for call in job._session.get.call_args_list]
        self.assertEqual(pages, [0, 1])

    def test_pagination_stops_on_last_page_flag(self):
        job = _make_job()
        full_page = [_make_task(str(n)) for n in range(100)]
        job._session.get.return_value = _response({"tasks": full_page, "last_page": True})

        self.assertEqual(len(list(job.list_items())), 100)
        self.assertEqual(job._session.get.call_count, 1)

    def test_duplicate_tasks_are_yielded_once(self):
        job = _make_job()
        job._session.get.return_value = _response({"tasks": [_make_task("t1"), _make_task("t1")]})

        self.assertEqual(len(list(job.list_items())), 1)

    def test_last_modified_from_date_updated(self):
        job = _make_job()
        job._session.get.return_value = _response({"tasks": [_make_task(date_updated="1700003600000")]})

        item = next(job.list_items())

        self.assertEqual(item.last_modified.isoformat(), "2023-11-14T23:13:20+00:00")

    def test_session_closed_after_listing(self):
        job = _make_job()
        job._session.get.return_value = _response({"tasks": [_make_task()]})

        list(job.list_items())

        job._session.close.assert_called_once()

    def test_session_closed_when_listing_fails(self):
        job = _make_job()
        job._session.get.return_value = _response({}, status=500)

        with self.assertRaises(requests.HTTPError):
            list(job.list_items())

        job._session.close.assert_called_once()

    def test_http_error_propagates(self):
        job = _make_job()
        job._session.get.return_value = _response({}, status=401)

        with self.assertRaises(requests.HTTPError):
            list(job.list_items())


class TestClickUpContent(unittest.TestCase):
    def test_content_uses_markdown_description_and_details(self):
        job = _make_job(retrieve_task_comments=False)

        content = job.get_raw_content(_item())

        self.assertIn("# Fix login", content)
        self.assertIn("**markdown** description", content)
        self.assertNotIn("plain description", content)
        self.assertIn("**Status:** in progress", content)
        self.assertIn("**Tags:** bug, urgent", content)
        self.assertIn("**Assignees:** bob <bob@example.com>", content)
        self.assertNotIn("## Comments", content)
        job._session.get.assert_not_called()

    def test_content_falls_back_to_plain_description(self):
        job = _make_job(retrieve_task_comments=False)

        content = job.get_raw_content(_item(_make_task(markdown_description=None)))

        self.assertIn("plain description", content)

    def test_content_tolerates_missing_fields(self):
        job = _make_job(retrieve_task_comments=False)

        content = job.get_raw_content(_item({"id": "t9"}))

        self.assertEqual(content, "# t9")

    def test_comments_rendered(self):
        job = _make_job()
        job._session.get.return_value = _response({"comments": [_make_comment("c1", "first comment")]})

        content = job.get_raw_content(_item())

        self.assertIn("## Comments", content)
        self.assertIn("**carol** (2023-11-14T22:15:00+00:00):\nfirst comment", content)

    def test_comments_paginated_with_start_and_start_id(self):
        job = _make_job()
        page1 = [_make_comment(str(n), date=str(2000 - n)) for n in range(25)]
        page2 = [_make_comment("older", date="100")]
        job._session.get.side_effect = [_response({"comments": page1}), _response({"comments": page2})]

        comments = job._get_comments(_item())

        self.assertEqual(len(comments), 26)
        second_params = job._session.get.call_args_list[1].kwargs["params"]
        self.assertEqual(second_params, {"start": page1[-1]["date"], "start_id": "24"})

    def test_comment_requests_use_task_comment_endpoint(self):
        job = _make_job()
        job._session.get.return_value = _response({"comments": []})

        job.get_raw_content(_item())

        self.assertEqual(job._session.get.call_args.args[0], "https://api.clickup.com/api/v2/task/t1/comment")


class TestClickUpIdentity(unittest.TestCase):
    def test_item_name_is_safe_and_bounded(self):
        job = _make_job()

        name = job.get_item_name(_item(_make_task("abc/12 3")))

        self.assertEqual(name, "clickup_task_abc_12_3")
        self.assertLessEqual(len(name), 255)

    def test_checksum_uses_update_time_without_requests(self):
        for retrieve_comments in (True, False):
            job = _make_job(retrieve_task_comments=retrieve_comments)

            checksum = job.get_item_checksum(_item())

            self.assertTrue(checksum.startswith("t1:1700003600000"))
            job._session.get.assert_not_called()

    def test_checksum_changes_when_task_is_updated(self):
        job = _make_job()

        before = job.get_item_checksum(_item(_make_task(date_updated="1700003600000")))
        after = job.get_item_checksum(_item(_make_task(date_updated="1700007200000")))

        self.assertNotEqual(before, after)

    def test_checksum_changes_when_comments_are_switched(self):
        with_comments = _make_job(retrieve_task_comments=True).get_item_checksum(_item())
        without_comments = _make_job(retrieve_task_comments=False).get_item_checksum(_item())

        self.assertNotEqual(with_comments, without_comments)

    def test_extra_metadata(self):
        job = _make_job()

        metadata = job.get_extra_metadata(_item(), "", {})

        self.assertEqual(metadata["task_id"], "t1")
        self.assertEqual(metadata["status"], "in progress")
        self.assertEqual(metadata["list"], "Sprint")
        self.assertEqual(metadata["project"], "Backend")
        self.assertEqual(metadata["folder"], "Engineering")
        self.assertEqual(metadata["space_id"], "90")
        self.assertEqual(metadata["tags"], ["bug", "urgent"])
        self.assertEqual(metadata["priority"], "high")
        self.assertEqual(metadata["creator"], "alice <alice@example.com>")
        self.assertEqual(metadata["assignees"], ["bob <bob@example.com>"])
        self.assertEqual(metadata["date_created"], "2023-11-14T22:13:20+00:00")
        self.assertIn("due_date", metadata)
        self.assertNotIn("date_closed", metadata)

    def test_extra_metadata_does_not_use_reserved_keys(self):
        reserved = {"source", "key", "checksum", "version", "format", "source_name", "file_name", "last_modified"}

        metadata = _make_job().get_extra_metadata(_item(), "", {})

        self.assertFalse(reserved & set(metadata))

    def test_extra_metadata_tolerates_nulls(self):
        job = _make_job()

        metadata = job.get_extra_metadata(_item({"id": "t2", "priority": None, "status": None, "tags": None}), "", {})

        self.assertEqual(metadata["priority"], "")
        self.assertEqual(metadata["tags"], [])
        self.assertNotIn("status", metadata)
        self.assertNotIn("list", metadata)
        self.assertNotIn(None, metadata.values())


class TestClickUpRateLimitAndRetries(unittest.TestCase):
    def test_session_receives_max_retries(self):
        with patch("tasks.clickup_ingestion.RetrySession") as session_cls:
            ClickUpIngestionJob(_make_config(max_retries=5))
        session_cls.assert_called_once_with(max_retries=5)

    def test_throttle_sleeps_when_limit_reached(self):
        job = _make_job(max_requests_per_minute=2)
        with (
            patch("tasks.clickup_ingestion.time.monotonic", side_effect=[0.0, 1.0, 2.0, 61.0]),
            patch("tasks.clickup_ingestion.time.sleep") as sleep,
        ):
            job._throttle()
            job._throttle()
            job._throttle()

        sleep.assert_called_once_with(58.0)

    def test_throttle_does_not_sleep_under_limit(self):
        job = _make_job(max_requests_per_minute=2)
        with (
            patch("tasks.clickup_ingestion.time.monotonic", side_effect=[0.0, 70.0, 71.0]),
            patch("tasks.clickup_ingestion.time.sleep") as sleep,
        ):
            for _ in range(3):
                job._throttle()

        sleep.assert_not_called()

    def test_exhausted_rate_limit_raises(self):
        job = _make_job()
        job._session.get.return_value = _response({}, status=429)

        with self.assertRaises(requests.HTTPError):
            job._get("team/1/task")


if __name__ == "__main__":
    unittest.main()
