import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from tasks.directory_ingestion import DirectoryIngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem


class TestDirectoryIngestionJob(unittest.TestCase):
    def setUp(self):
        self.markitdown_patcher = patch("tasks.directory_ingestion.MarkItDown")
        self.mock_markitdown_class = self.markitdown_patcher.start()
        self.mock_md = self.mock_markitdown_class.return_value

    def tearDown(self):
        self.markitdown_patcher.stop()

    def test_source_type(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            job = DirectoryIngestionJob(
                {"name": "local", "config": {"path": temp_dir}}
            )
            self.assertEqual(job.source_type, "directory")

    def test_init_requires_path(self):
        with self.assertRaises(ValueError):
            DirectoryIngestionJob({"name": "local", "config": {}})

    def test_init_rejects_path_to_file(self):
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            file_path = f.name
        try:
            with self.assertRaises(ValueError) as ctx:
                DirectoryIngestionJob(
                    {"name": "local", "config": {"path": file_path}}
                )
            self.assertIn("not a directory", str(ctx.exception))
        finally:
            Path(file_path).unlink(missing_ok=True)

    def test_list_items_recursive(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            (base / "root.txt").write_text("root", encoding="utf-8")
            nested_dir = base / "nested"
            nested_dir.mkdir()
            (nested_dir / "child.md").write_text("child", encoding="utf-8")

            job = DirectoryIngestionJob(
                {"name": "local", "config": {"path": temp_dir}}
            )

            items = list(job.list_items())

            self.assertEqual(len(items), 2)
            self.assertTrue(items[0].id.startswith("file://"))
            self.assertIsInstance(items[0].source_ref, Path)
            self.assertIsNotNone(items[0].last_modified)
            self.assertIsNotNone(
                items[0].last_modified.tzinfo,
                "last_modified must be timezone-aware",
            )

    def test_list_items_non_recursive(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            (base / "root.txt").write_text("root", encoding="utf-8")
            nested_dir = base / "nested"
            nested_dir.mkdir()
            (nested_dir / "child.md").write_text("child", encoding="utf-8")

            job = DirectoryIngestionJob(
                {
                    "name": "local",
                    "config": {"path": temp_dir, "recursive": False},
                }
            )

            items = list(job.list_items())

            self.assertEqual(len(items), 1)
            self.assertEqual(Path(items[0].source_ref).name, "root.txt")

    def test_list_items_recursive_false_string(self):
        """Config from env can give recursive as string 'false'; must be respected."""
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            (base / "root.txt").write_text("root", encoding="utf-8")
            nested_dir = base / "nested"
            nested_dir.mkdir()
            (nested_dir / "child.md").write_text("child", encoding="utf-8")

            job = DirectoryIngestionJob(
                {
                    "name": "local",
                    "config": {"path": temp_dir, "recursive": "false"},
                }
            )

            items = list(job.list_items())
            self.assertEqual(len(items), 1)
            self.assertEqual(Path(items[0].source_ref).name, "root.txt")

    def test_list_items_filter_by_extensions(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            (base / "one.txt").write_text("one", encoding="utf-8")
            (base / "two.pdf").write_text("two", encoding="utf-8")
            nested_dir = base / "nested"
            nested_dir.mkdir()
            (nested_dir / "three.md").write_text("three", encoding="utf-8")

            job = DirectoryIngestionJob(
                {
                    "name": "local",
                    "config": {"path": temp_dir, "filter": "txt,md"},
                }
            )

            items = list(job.list_items())
            names = [Path(item.source_ref).name for item in items]

            self.assertEqual(len(items), 2)
            self.assertIn("one.txt", names)
            self.assertIn("three.md", names)
            self.assertNotIn("two.pdf", names)

    def test_list_items_filter_normalizes_dots_and_case(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            (base / "one.txt").write_text("one", encoding="utf-8")
            (base / "two.md").write_text("two", encoding="utf-8")
            (base / "three.PDF").write_text("three", encoding="utf-8")

            job = DirectoryIngestionJob(
                {
                    "name": "local",
                    "config": {"path": temp_dir, "filter": ".TXT, .pdf"},
                }
            )

            items = list(job.list_items())
            names = [Path(item.source_ref).name for item in items]

            self.assertEqual(len(items), 2)
            self.assertIn("one.txt", names)
            self.assertIn("three.PDF", names)
            self.assertNotIn("two.md", names)

    def test_get_raw_content_uses_markdown_conversion(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "doc.txt"
            file_path.write_text("raw text", encoding="utf-8")

            conversion_result = Mock(text_content="Converted markdown")
            self.mock_md.convert_stream.return_value = conversion_result

            job = DirectoryIngestionJob(
                {"name": "local", "config": {"path": temp_dir}}
            )

            item = IngestionItem(id=f"file://{file_path}", source_ref=file_path)
            result = job.get_raw_content(item)

            self.assertEqual(result, "Converted markdown")
            self.mock_md.convert_stream.assert_called_once()

    def test_get_raw_content_falls_back_on_empty_conversion(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "doc.txt"
            file_path.write_text("fallback text", encoding="utf-8")

            conversion_result = Mock(text_content="   ")
            self.mock_md.convert_stream.return_value = conversion_result

            job = DirectoryIngestionJob(
                {"name": "local", "config": {"path": temp_dir}}
            )

            item = IngestionItem(id=f"file://{file_path}", source_ref=file_path)
            result = job.get_raw_content(item)

            self.assertEqual(result, "fallback text")

    def test_get_raw_content_falls_back_on_conversion_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "doc.txt"
            file_path.write_text("fallback text", encoding="utf-8")

            self.mock_md.convert_stream.side_effect = ValueError("bad markdown")

            job = DirectoryIngestionJob(
                {"name": "local", "config": {"path": temp_dir}}
            )

            item = IngestionItem(id=f"file://{file_path}", source_ref=file_path)
            result = job.get_raw_content(item)

            self.assertEqual(result, "fallback text")

    def test_get_raw_content_returns_empty_on_read_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            missing_path = Path(temp_dir) / "missing.txt"
            job = DirectoryIngestionJob(
                {"name": "local", "config": {"path": temp_dir}}
            )

            item = IngestionItem(id=f"file://{missing_path}", source_ref=missing_path)
            result = job.get_raw_content(item)

            self.assertEqual(result, "")

    def test_get_item_name_uses_relative_sanitized_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            nested_dir = base / "A folder"
            nested_dir.mkdir()
            file_path = nested_dir / "Angstrom ?.txt"
            file_path.write_text("x", encoding="utf-8")

            job = DirectoryIngestionJob(
                {"name": "local", "config": {"path": temp_dir}}
            )
            item = IngestionItem(id=f"file://{file_path}", source_ref=file_path)

            self.assertEqual(job.get_item_name(item), "A_folder__Angstrom_.txt")

    def test_get_item_name_distinguishes_path_from_underscore(self):
        """a/b.txt and a_b.txt must produce different keys (no collision)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            sub = base / "a"
            sub.mkdir()
            (sub / "b.txt").write_text("nested", encoding="utf-8")
            (base / "a_b.txt").write_text("flat", encoding="utf-8")

            job = DirectoryIngestionJob(
                {"name": "local", "config": {"path": temp_dir}}
            )
            items = list(job.list_items())
            names = [job.get_item_name(item) for item in items]
            self.assertEqual(len(names), 2)
            self.assertIn("a__b.txt", names)
            self.assertIn("a_b.txt", names)
            self.assertNotEqual(names[0], names[1])

    def test_get_item_name_fallback_when_outside_directory(self):
        """When source_ref resolves outside job directory, use filename only."""
        with tempfile.TemporaryDirectory() as job_dir:
            with tempfile.TemporaryDirectory() as other_dir:
                job = DirectoryIngestionJob(
                    {"name": "local", "config": {"path": job_dir}}
                )
                outside_file = Path(other_dir) / "external.txt"
                outside_file.write_text("x", encoding="utf-8")
                item = IngestionItem(
                    id=f"file://{outside_file}",
                    source_ref=outside_file,
                )
                name = job.get_item_name(item)
                self.assertEqual(name, "external.txt")


if __name__ == "__main__":
    unittest.main()
