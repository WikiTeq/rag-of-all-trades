import io
import unittest
from datetime import datetime
from unittest.mock import Mock, patch

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.s3_ingestion import S3IngestionJob


class TestS3IngestionJob(unittest.TestCase):
    """Tests for S3 ingestion job behavior with mocked S3 responses."""

    def setUp(self):
        self.mock_s3 = Mock()
        self.mock_md = Mock()
        self.get_s3_client_patcher = patch(
            "tasks.s3_ingestion.get_s3_client",
            return_value=(self.mock_s3, None),
        )
        self.markitdown_patcher = patch(
            "tasks.s3_ingestion.MarkItDown",
            return_value=self.mock_md,
        )
        self.get_s3_client_patcher.start()
        self.markitdown_patcher.start()
        self.config = {"name": "test", "config": {"buckets": ["bucket-a"]}}

    def tearDown(self):
        self.get_s3_client_patcher.stop()
        self.markitdown_patcher.stop()

    def test_source_type(self):
        job = S3IngestionJob(self.config)
        self.assertEqual(job.source_type, "s3")

    def test_init_buckets_from_string(self):
        job = S3IngestionJob({"name": "test", "config": {"buckets": " a, b, ,c "}})
        self.assertEqual(job.buckets, ["a", "b", "c"])

    def test_sanitize_s3_key_normalizes(self):
        job = S3IngestionJob(self.config)
        key = "Angstrom / space\\test?.txt"
        self.assertEqual(job.sanitize_s3_key(key), "Angstrom_space_test.txt")

    def test_sanitize_s3_key_truncates(self):
        job = S3IngestionJob(self.config)
        key = "a" * 300
        self.assertEqual(job.sanitize_s3_key(key), "a" * 255)

    def test_list_items_pagination_and_filters(self):
        last_modified_1 = datetime(2024, 1, 1)
        last_modified_2 = datetime(2024, 1, 2)
        last_modified_3 = datetime(2024, 1, 3)
        self.mock_s3.list_objects_v2.side_effect = [
            {
                "Contents": [
                    {"Key": "folder/"},
                    {"Key": "file1.txt", "LastModified": last_modified_1},
                    {"Key": "file2.md", "LastModified": last_modified_2},
                ],
                "IsTruncated": True,
                "NextContinuationToken": "token1",
            },
            {
                "Contents": [
                    {"Key": "file3.txt", "LastModified": last_modified_3},
                ],
                "IsTruncated": False,
            },
        ]
        job = S3IngestionJob(self.config)

        items = list(job.list_items())

        self.assertEqual(len(items), 3)
        self.assertEqual(items[0].id, "s3://bucket-a/file1.txt")
        self.assertEqual(items[1].id, "s3://bucket-a/file2.md")
        self.assertEqual(items[2].source_ref, ("bucket-a", "file3.txt"))

        first_call = self.mock_s3.list_objects_v2.call_args_list[0][1]
        second_call = self.mock_s3.list_objects_v2.call_args_list[1][1]
        self.assertNotIn("ContinuationToken", first_call)
        self.assertEqual(second_call["ContinuationToken"], "token1")

    def test_get_raw_content_uses_markdown_conversion(self):
        self.mock_s3.get_object.return_value = {"Body": io.BytesIO(b"raw bytes")}
        conversion_result = Mock(text_content="Converted text")
        self.mock_md.convert_stream.return_value = conversion_result

        job = S3IngestionJob(self.config)
        item = IngestionItem(
            id="s3://bucket-a/file1.txt",
            source_ref=("bucket-a", "file1.txt"),
        )
        result = job.get_raw_content(item)

        self.assertEqual(result, "Converted text")
        self.mock_md.convert_stream.assert_called_once()

    def test_get_raw_content_falls_back_on_empty_conversion(self):
        self.mock_s3.get_object.return_value = {"Body": io.BytesIO(b"raw text")}
        conversion_result = Mock(text_content="   ")
        self.mock_md.convert_stream.return_value = conversion_result

        job = S3IngestionJob(self.config)
        item = IngestionItem(
            id="s3://bucket-a/file1.txt",
            source_ref=("bucket-a", "file1.txt"),
        )
        result = job.get_raw_content(item)

        self.assertEqual(result, "raw text")

    def test_get_raw_content_falls_back_on_conversion_error(self):
        self.mock_s3.get_object.return_value = {"Body": io.BytesIO(b"raw text")}
        self.mock_md.convert_stream.side_effect = ValueError("bad markdown")

        job = S3IngestionJob(self.config)
        item = IngestionItem(
            id="s3://bucket-a/file1.txt",
            source_ref=("bucket-a", "file1.txt"),
        )
        with self.assertLogs("tasks.s3_ingestion", level="WARNING") as cm:
            result = job.get_raw_content(item)

        self.assertEqual(result, "raw text")
        self.assertIn(
            "WARNING:tasks.s3_ingestion:[bucket-a/file1.txt] Markdown conversion failed: bad markdown. Using raw text.",
            cm.output,
        )

    def test_get_raw_content_returns_empty_on_s3_error(self):
        self.mock_s3.get_object.side_effect = Exception("boom")

        job = S3IngestionJob(self.config)
        item = IngestionItem(
            id="s3://bucket-a/file1.txt",
            source_ref=("bucket-a", "file1.txt"),
        )
        with self.assertLogs("tasks.s3_ingestion", level="ERROR") as cm:
            result = job.get_raw_content(item)

        self.assertEqual(result, "")
        self.assertIn(
            "ERROR:tasks.s3_ingestion:[bucket-a/file1.txt] Failed to fetch content: boom",
            cm.output,
        )
