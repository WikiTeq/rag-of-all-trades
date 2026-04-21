import json
import unittest
from datetime import datetime
from unittest.mock import patch

import requests as req_mod

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.slab_ingestion import SlabIngestionJob


def _make_job(api_token="test-token", topic_ids=None):
    cfg: dict = {"api_token": api_token}
    if topic_ids is not None:
        cfg["topic_ids"] = topic_ids
    config = {"name": "slab-test", "config": cfg}
    with patch("tasks.base.MetadataTracker"), patch("tasks.base.VectorStoreManager"):
        return SlabIngestionJob(config)


class TestSlabIngestionInit(unittest.TestCase):
    def test_valid_config(self):
        job = _make_job()
        self.assertEqual(job.topic_ids, [])

    def test_topic_ids_list(self):
        job = _make_job(topic_ids=["t1", "t2"])
        self.assertEqual(job.topic_ids, ["t1", "t2"])

    def test_topic_ids_csv_string(self):
        job = _make_job(topic_ids="t1, t2")
        self.assertEqual(job.topic_ids, ["t1", "t2"])

    def test_missing_api_token_raises(self):
        with self.assertRaises(ValueError, msg="api_token is required"):
            _make_job(api_token="")


class TestSlabListItemsAllPosts(unittest.TestCase):
    def _search_response(self, posts, has_next=False, cursor=None):
        edges = [{"node": {"post": p}} for p in posts]
        return {
            "data": {
                "search": {
                    "edges": edges,
                    "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
                }
            }
        }

    def test_list_all_single_page(self):
        job = _make_job()
        posts = [
            {"id": "p1", "title": "Post 1", "content": "Body 1", "updatedAt": "2024-01-01T00:00:00+00:00"},
            {"id": "p2", "title": "Post 2", "content": "Body 2", "updatedAt": None},
        ]
        with patch.object(job._client, "execute", return_value=self._search_response(posts)):
            items = list(job.list_items())

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].id, "p1")
        self.assertEqual(items[1].id, "p2")
        self.assertIsInstance(items[0].last_modified, datetime)
        self.assertIsNone(items[1].last_modified)

    def test_list_all_paginated(self):
        job = _make_job()
        page1 = [{"id": "p1", "title": "A", "content": "", "updatedAt": None}]
        page2 = [{"id": "p2", "title": "B", "content": "", "updatedAt": None}]
        responses = [
            self._search_response(page1, has_next=True, cursor="cur1"),
            self._search_response(page2, has_next=False),
        ]
        with patch.object(job._client, "execute", side_effect=responses):
            items = list(job.list_items())

        self.assertEqual([i.id for i in items], ["p1", "p2"])

    def test_skips_posts_without_id(self):
        job = _make_job()
        posts = [{"id": "", "title": "No ID", "content": "", "updatedAt": None}]
        with patch.object(job._client, "execute", return_value=self._search_response(posts)):
            items = list(job.list_items())
        self.assertEqual(items, [])


class TestSlabListItemsByTopics(unittest.TestCase):
    def _topic_response(self, post_stubs, topic_id="topic1", topic_name="Engineering"):
        return {
            "data": {
                "topic": {
                    "id": topic_id,
                    "name": topic_name,
                    "parent": None,
                    "ancestors": [],
                    "posts": post_stubs,
                }
            }
        }

    def _post_response(self, post):
        return {"data": {"post": post}}

    def test_list_by_topic(self):
        job = _make_job(topic_ids=["topic1"])
        stub = {"id": "p1", "title": "Post 1", "updatedAt": None}
        full = {
            "id": "p1",
            "title": "Post 1",
            "content": "Full content",
            "updatedAt": "2024-06-01T12:00:00+00:00",
        }
        responses = [self._topic_response([stub]), self._post_response(full)]
        with patch.object(job._client, "execute", side_effect=responses):
            items = list(job.list_items())

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].id, "p1")
        topic_meta = items[0].source_ref["_topic_meta"]
        self.assertEqual(topic_meta["id"], "topic1")
        self.assertEqual(topic_meta["name"], "Engineering")

    def test_skips_missing_post(self):
        job = _make_job(topic_ids=["topic1"])
        stub = {"id": "p1", "title": "Post 1", "updatedAt": None}
        responses = [self._topic_response([stub]), {"data": {"post": {}}}]
        with patch.object(job._client, "execute", side_effect=responses):
            items = list(job.list_items())
        self.assertEqual(items, [])


class TestSlabGetRawContent(unittest.TestCase):
    def test_content_with_title(self):
        job = _make_job()
        item = IngestionItem(
            id="p1",
            source_ref={"id": "p1", "title": "My Post", "content": "Hello world"},
        )
        result = job.get_raw_content(item)
        self.assertIn("# My Post", result)
        self.assertIn("Hello world", result)

    def test_quill_delta_content(self):
        job = _make_job()
        delta = json.dumps([{"insert": "Hello "}, {"insert": "world\n"}])
        item = IngestionItem(
            id="p1",
            source_ref={"id": "p1", "title": "Post", "content": delta},
        )
        result = job.get_raw_content(item)
        self.assertIn("Hello world", result)

    def test_empty_content(self):
        job = _make_job()
        item = IngestionItem(id="p1", source_ref={"id": "p1", "title": "", "content": ""})
        result = job.get_raw_content(item)
        self.assertEqual(result, "")


class TestSlabGetItemName(unittest.TestCase):
    def test_safe_name(self):
        job = _make_job()
        item = IngestionItem(id="abc123", source_ref={"title": "My Post Title"})
        name = job.get_item_name(item)
        self.assertIn("My_Post_Title", name)
        self.assertIn("abc123", name)
        self.assertLessEqual(len(name), 255)

    def test_falls_back_to_id(self):
        job = _make_job()
        item = IngestionItem(id="abc123", source_ref={"title": ""})
        name = job.get_item_name(item)
        self.assertIn("abc123", name)


class TestSlabGetExtraMetadata(unittest.TestCase):
    def test_metadata_with_topic(self):
        job = _make_job()
        topic_meta = {
            "id": "t1",
            "name": "Engineering",
            "parent_id": "",
            "parent_name": "",
            "ancestors": [],
        }
        item = IngestionItem(
            id="abc123",
            source_ref={"title": "My Post", "_topic_meta": topic_meta},
        )
        meta = job.get_extra_metadata(item, "", {})
        self.assertIn("abc123", meta["url"])
        self.assertEqual(meta["title"], "My Post")
        self.assertEqual(meta["topic_id"], "t1")
        self.assertEqual(meta["topic_name"], "Engineering")
        self.assertEqual(meta["topic_ancestors"], [])

    def test_metadata_no_topic(self):
        job = _make_job()
        item = IngestionItem(id="abc123", source_ref={"title": "Post"})
        meta = job.get_extra_metadata(item, "", {})
        self.assertIn("abc123", meta["url"])
        self.assertEqual(meta["topic_id"], "")
        self.assertEqual(meta["topic_name"], "")
        self.assertEqual(meta["topic_ancestors"], [])

    def test_metadata_with_parent_topic(self):
        job = _make_job()
        topic_meta = {
            "id": "t2",
            "name": "Backend",
            "parent_id": "t1",
            "parent_name": "Engineering",
            "ancestors": [{"id": "t1", "name": "Engineering"}],
        }
        item = IngestionItem(id="abc123", source_ref={"title": "Post", "_topic_meta": topic_meta})
        meta = job.get_extra_metadata(item, "", {})
        self.assertEqual(meta["topic_parent_id"], "t1")
        self.assertEqual(meta["topic_parent_name"], "Engineering")
        self.assertEqual(meta["topic_ancestors"], [{"id": "t1", "name": "Engineering"}])


class TestSlabGraphqlRetry(unittest.TestCase):
    def test_retries_on_timeout(self):
        job = _make_job()
        with patch("tasks.slab_ingestion.requests.post") as mock_post, patch("tasks.slab_ingestion.time.sleep"):
            mock_post.side_effect = req_mod.exceptions.Timeout
            with self.assertRaises(RuntimeError):
                job._client.execute("{ test }")
            self.assertEqual(mock_post.call_count, job._client.max_retries)


if __name__ == "__main__":
    unittest.main()
