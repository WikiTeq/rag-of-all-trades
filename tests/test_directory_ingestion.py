import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from tasks.directory_ingestion import DirectoryIngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem


class TestDirectoryIngestionJob(unittest.TestCase):
    def setUp(self):
        self.reader_patcher = patch("tasks.directory_ingestion.SimpleDirectoryReader")
        self.mock_reader_class = self.reader_patcher.start()
        self.mock_directory_reader = Mock()
        self.mock_directory_reader.list_resources.return_value = []
        self.mock_reader_class.return_value = self.mock_directory_reader

    def tearDown(self):
        self.reader_patcher.stop()

    def test_source_type(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            job = DirectoryIngestionJob(
                {"name": "local", "config": {"path": temp_dir}}
            )
            self.assertEqual(job.source_type, "directory")

    def test_init_requires_path(self):
        with self.assertRaises(ValueError):
            DirectoryIngestionJob({"name": "local", "config": {}})

    def test_list_items_recursive(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            (base / "root.txt").write_text("root", encoding="utf-8")
            nested_dir = base / "nested"
            nested_dir.mkdir()
            (nested_dir / "child.md").write_text("child", encoding="utf-8")
            self.mock_directory_reader.list_resources.return_value = [
                str(base / "root.txt"),
                str(nested_dir / "child.md"),
            ]

            job = DirectoryIngestionJob(
                {"name": "local", "config": {"path": temp_dir}}
            )

            items = list(job.list_items())

            self.assertEqual(len(items), 2)
            self.mock_reader_class.assert_called_with(
                input_dir=str(Path(temp_dir).resolve()),
                recursive=True,
                required_exts=None,
                exclude_hidden=True,
                exclude_empty=False,
                num_files_limit=None,
                encoding="utf-8",
                errors="ignore",
                raise_on_error=True,
            )
            self.assertTrue(items[0].id.startswith("file://"))
            self.assertIsInstance(items[0].source_ref, Path)
            self.assertIsNotNone(items[0].last_modified)

    def test_list_items_non_recursive(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            (base / "root.txt").write_text("root", encoding="utf-8")
            nested_dir = base / "nested"
            nested_dir.mkdir()
            (nested_dir / "child.md").write_text("child", encoding="utf-8")
            self.mock_directory_reader.list_resources.return_value = [
                str(base / "root.txt")
            ]

            job = DirectoryIngestionJob(
                {
                    "name": "local",
                    "config": {"path": temp_dir, "recursive": False},
                }
            )

            items = list(job.list_items())

            self.assertEqual(len(items), 1)
            self.assertEqual(Path(items[0].source_ref).name, "root.txt")
            self.assertEqual(job.recursive, False)

    def test_list_items_resolves_relative_resources_from_base_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            (base / "root.txt").write_text("root", encoding="utf-8")
            self.mock_directory_reader.list_resources.return_value = ["root.txt"]

            job = DirectoryIngestionJob(
                {"name": "local", "config": {"path": temp_dir}}
            )

            items = list(job.list_items())

            self.assertEqual(len(items), 1)
            self.assertEqual(items[0].source_ref, (base / "root.txt").resolve())

    def test_filter_is_translated_to_required_exts(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            job = DirectoryIngestionJob(
                {
                    "name": "local",
                    "config": {"path": temp_dir, "filter": "txt,md"},
                }
            )

            self.assertEqual(job.required_exts, [".md", ".txt"])

    def test_list_items_filter_normalizes_dots_and_case(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            job = DirectoryIngestionJob(
                {
                    "name": "local",
                    "config": {"path": temp_dir, "filter": ".TXT, .pdf"},
                }
            )

            self.assertEqual(job.required_exts, [".pdf", ".txt"])

    def test_get_raw_content_uses_simple_directory_reader(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "doc.txt"
            file_path.write_text("raw text", encoding="utf-8")

            self.mock_directory_reader.load_resource.return_value = [Mock(text="Converted text")]

            job = DirectoryIngestionJob(
                {"name": "local", "config": {"path": temp_dir}}
            )

            item = IngestionItem(id=f"file://{file_path}", source_ref=file_path)
            result = job.get_raw_content(item)

            self.assertEqual(result, "Converted text")
            self.mock_directory_reader.load_resource.assert_called_once_with(str(file_path))

    def test_get_raw_content_joins_multiple_documents(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "doc.txt"
            file_path.write_text("ignored", encoding="utf-8")

            self.mock_directory_reader.load_resource.return_value = [
                Mock(text="Part 1"),
                Mock(text="Part 2"),
            ]

            job = DirectoryIngestionJob(
                {"name": "local", "config": {"path": temp_dir}}
            )

            item = IngestionItem(id=f"file://{file_path}", source_ref=file_path)
            result = job.get_raw_content(item)

            self.assertEqual(result, "Part 1\n\nPart 2")

    def test_get_raw_content_removes_nul_chars_from_reader_output(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "doc.txt"
            file_path.write_text("ignored", encoding="utf-8")
            self.mock_directory_reader.load_resource.return_value = [Mock(text="a\x00b")]

            job = DirectoryIngestionJob(
                {"name": "local", "config": {"path": temp_dir}}
            )
            item = IngestionItem(id=f"file://{file_path}", source_ref=file_path)
            result = job.get_raw_content(item)

            self.assertEqual(result, "ab")

    def test_get_raw_content_returns_empty_on_loader_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "doc.txt"
            file_path.write_text("fallback text", encoding="utf-8")

            self.mock_directory_reader.load_resource.side_effect = ValueError("bad loader")

            job = DirectoryIngestionJob(
                {"name": "local", "config": {"path": temp_dir}}
            )

            item = IngestionItem(id=f"file://{file_path}", source_ref=file_path)
            with patch("tasks.directory_ingestion.logger.warning") as mock_warning:
                result = job.get_raw_content(item)

            self.assertEqual(result, "")
            mock_warning.assert_called_once()
            self.assertIn(str(file_path), mock_warning.call_args[0][0])
            self.assertIn("SimpleDirectoryReader failed", mock_warning.call_args[0][0])
            self.assertIn("bad loader", mock_warning.call_args[0][0])

    def test_get_raw_content_returns_empty_when_reader_returns_no_docs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "doc.txt"
            file_path.write_text("fallback text", encoding="utf-8")
            self.mock_directory_reader.load_resource.return_value = []

            job = DirectoryIngestionJob(
                {"name": "local", "config": {"path": temp_dir}}
            )

            item = IngestionItem(id=f"file://{file_path}", source_ref=file_path)
            result = job.get_raw_content(item)

            self.assertEqual(result, "")

    def test_get_raw_content_returns_empty_on_loader_error_for_missing_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            missing_path = Path(temp_dir) / "missing.txt"
            self.mock_directory_reader.load_resource.side_effect = ValueError("missing file")
            job = DirectoryIngestionJob(
                {"name": "local", "config": {"path": temp_dir}}
            )

            item = IngestionItem(id=f"file://{missing_path}", source_ref=missing_path)
            with patch("tasks.directory_ingestion.logger.warning") as mock_warning:
                result = job.get_raw_content(item)

            self.assertEqual(result, "")
            mock_warning.assert_called_once()
            self.assertIn(str(missing_path), mock_warning.call_args[0][0])
            self.assertIn("SimpleDirectoryReader failed", mock_warning.call_args[0][0])
            self.assertIn("missing file", mock_warning.call_args[0][0])

    def test_config_parses_bool_strings_and_num_files_limit_with_forced_raise_on_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            job = DirectoryIngestionJob(
                {
                    "name": "local",
                    "config": {
                        "path": temp_dir,
                        "recursive": "false",
                        "exclude_hidden": "no",
                        "exclude_empty": "yes",
                        "raise_on_error": "0",
                        "num_files_limit": "7",
                    },
                }
            )

            self.assertEqual(job.recursive, False)
            self.assertEqual(job.exclude_hidden, False)
            self.assertEqual(job.exclude_empty, True)
            self.assertEqual(job.raise_on_error, True)
            self.assertEqual(job.num_files_limit, 7)

    def test_config_forces_errors_ignore(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            job = DirectoryIngestionJob(
                {
                    "name": "local",
                    "config": {
                        "path": temp_dir,
                        "errors": "replace",
                    },
                }
            )

            self.assertEqual(job.errors, "ignore")

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

            self.assertEqual(job.get_item_name(item), "A_folder_Angstrom_.txt")


if __name__ == "__main__":
    unittest.main()
