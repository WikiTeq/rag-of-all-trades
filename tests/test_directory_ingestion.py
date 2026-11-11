import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from pydantic import ValidationError

from tasks.directory_ingestion import DirectoryIngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.helper_classes.ingestion_result import IngestionItemResult, IngestionItemStatus


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
            job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})
            self.assertEqual(job.source_type, "directory")

    def test_init_requires_path(self):
        with self.assertRaises(ValidationError):
            DirectoryIngestionJob({"name": "local", "config": {}})

    def test_init_rejects_num_files_limit_zero_or_negative(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            for invalid in (0, -1):
                with self.subTest(num_files_limit=invalid):
                    with self.assertRaises(ValidationError):
                        DirectoryIngestionJob(
                            {
                                "name": "local",
                                "config": {
                                    "path": temp_dir,
                                    "num_files_limit": invalid,
                                },
                            }
                        )

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

            job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})

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
            self.mock_directory_reader.list_resources.return_value = [str(base / "root.txt")]

            job = DirectoryIngestionJob(
                {
                    "name": "local",
                    "config": {"path": temp_dir, "recursive": False},
                }
            )

            items = list(job.list_items())

            self.assertEqual(len(items), 1)
            self.assertEqual(Path(items[0].source_ref).name, "root.txt")
            self.assertEqual(job.connector_config.recursive, False)

    def test_list_items_resolves_relative_resources_from_base_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            (base / "root.txt").write_text("root", encoding="utf-8")
            self.mock_directory_reader.list_resources.return_value = ["root.txt"]

            job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})

            items = list(job.list_items())

            self.assertEqual(len(items), 1)
            self.assertEqual(items[0].source_ref, (base / "root.txt").resolve())

    def test_required_exts_normalizes_extensions(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            job = DirectoryIngestionJob(
                {
                    "name": "local",
                    "config": {"path": temp_dir, "required_exts": "txt,md"},
                }
            )

            self.assertEqual(job.connector_config.required_exts, [".md", ".txt"])

    def test_required_exts_normalizes_dots_and_case(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            job = DirectoryIngestionJob(
                {
                    "name": "local",
                    "config": {"path": temp_dir, "required_exts": ".TXT, .pdf"},
                }
            )

            self.assertEqual(job.connector_config.required_exts, [".pdf", ".txt"])

    def test_required_exts_empty_string_normalizes_to_none(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            job = DirectoryIngestionJob(
                {
                    "name": "local",
                    "config": {"path": temp_dir, "required_exts": ""},
                }
            )
            self.assertIsNone(job.connector_config.required_exts)

    def test_required_exts_accepts_list_input(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            job = DirectoryIngestionJob(
                {
                    "name": "local",
                    "config": {"path": temp_dir, "required_exts": ["TXT", " md ", None, ""]},
                }
            )

            self.assertEqual(job.connector_config.required_exts, [".md", ".txt"])

    def test_get_raw_content_uses_simple_directory_reader(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "doc.txt"
            file_path.write_text("raw text", encoding="utf-8")

            self.mock_directory_reader.load_resource.return_value = [Mock(text="Converted text")]

            job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})

            item = IngestionItem(id=f"file://{file_path}", source_ref=file_path)
            result = job.get_raw_content(item)

            self.assertEqual(result, "Converted text")
            self.mock_directory_reader.load_resource.assert_called_once_with(str(file_path))

    def test_get_raw_content_transcribes_video(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            video = Path(temp_dir) / "talk.mkv"
            video.write_bytes(b"video")
            job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})
            item = IngestionItem(id=f"file://{video}", source_ref=video)

            with patch.object(job, "convert_video_to_markdown", return_value="[00:00-00:01] Hi") as convert:
                result = job.get_raw_content(item)

            self.assertEqual(result, IngestionItemResult(IngestionItemStatus.SUCCESS, "[00:00-00:01] Hi"))
            convert.assert_called_once_with(video)
            self.mock_directory_reader.load_resource.assert_not_called()

    def test_get_raw_content_returns_success_for_video_without_speech(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            video = Path(temp_dir) / "silent.mp4"
            video.write_bytes(b"video")
            job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})
            item = IngestionItem(id=f"file://{video}", source_ref=video)

            with patch.object(job, "convert_video_to_markdown", return_value=""):
                result = job.get_raw_content(item)

            self.assertEqual(result, IngestionItemResult(IngestionItemStatus.SUCCESS, ""))

    def test_get_raw_content_returns_fail_when_video_conversion_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            video = Path(temp_dir) / "broken.mp4"
            video.write_bytes(b"video")
            job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})
            item = IngestionItem(id=f"file://{video}", source_ref=video)

            with patch.object(job, "convert_video_to_markdown", side_effect=RuntimeError("conversion failed")):
                result = job.get_raw_content(item)

            self.assertEqual(result, IngestionItemResult(IngestionItemStatus.FAIL, errors=("conversion failed",)))

    def test_get_item_checksum_hashes_video_bytes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            video = Path(temp_dir) / "talk.webm"
            video.write_bytes(b"video bytes")
            job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})
            item = IngestionItem(id=f"file://{video}", source_ref=video)

            expected = hashlib.md5(b"video bytes", usedforsecurity=False).hexdigest()
            self.assertEqual(job.get_item_checksum(item), expected)

            video.write_bytes(b"other bytes")
            self.assertNotEqual(job.get_item_checksum(item), expected)

    def test_get_item_checksum_hashes_document_bytes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            doc = Path(temp_dir) / "doc.txt"
            doc.write_bytes(b"text")
            job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})
            item = IngestionItem(id=f"file://{doc}", source_ref=doc)

            expected = hashlib.md5(b"text", usedforsecurity=False).hexdigest()
            self.assertEqual(job.get_item_checksum(item), expected)

            doc.write_bytes(b"changed")
            self.assertNotEqual(job.get_item_checksum(item), expected)

    def test_get_item_checksum_is_none_when_file_unreadable(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})
            missing = Path(temp_dir) / "gone.mp4"

            self.assertIsNone(job.get_item_checksum(IngestionItem(id="x", source_ref=missing)))

    def test_process_item_records_silent_video_once_and_skips_unchanged_run(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            video = Path(temp_dir) / "silent.mp4"
            video.write_bytes(b"video bytes")
            job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})
            item = IngestionItem(id=f"file://{video}", source_ref=video)
            job.vector_manager = Mock()
            job.metadata_tracker = Mock()
            job.metadata_tracker.get_latest_record.return_value = None

            with patch.object(job, "convert_video_to_markdown", return_value="") as convert:
                self.assertEqual(job.process_item(item), 0)

            expected = hashlib.md5(b"video bytes", usedforsecurity=False).hexdigest()
            job.vector_manager.insert_documents.assert_not_called()
            args = job.metadata_tracker.record_metadata.call_args.args
            self.assertEqual(args[:4], ("silent.mp4", expected, 1, 0))
            convert.assert_called_once_with(video)

            job.metadata_tracker.record_metadata.reset_mock()
            job.metadata_tracker.get_latest_record.return_value = Mock(checksum=expected, version=1)
            job._seen.clear()

            with patch.object(job, "convert_video_to_markdown", return_value="") as convert:
                self.assertEqual(job.process_item(item), 0)

            convert.assert_not_called()
            job.metadata_tracker.record_metadata.assert_not_called()

    def test_get_raw_content_joins_multiple_documents(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "doc.txt"
            file_path.write_text("ignored", encoding="utf-8")

            self.mock_directory_reader.load_resource.return_value = [
                Mock(text="Part 1"),
                Mock(text="Part 2"),
            ]

            job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})

            item = IngestionItem(id=f"file://{file_path}", source_ref=file_path)
            result = job.get_raw_content(item)

            self.assertEqual(result, "Part 1\n\nPart 2")

    def test_get_raw_content_returns_empty_on_loader_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "doc.txt"
            file_path.write_text("fallback text", encoding="utf-8")

            self.mock_directory_reader.load_resource.side_effect = ValueError("bad loader")

            job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})

            item = IngestionItem(id=f"file://{file_path}", source_ref=file_path)
            with patch("tasks.directory_ingestion.logger.warning") as mock_warning:
                result = job.get_raw_content(item)

            self.assertEqual(result, "")
            mock_warning.assert_called_once()
            args = mock_warning.call_args[0]
            self.assertEqual(args[1], file_path)
            self.assertIn("SimpleDirectoryReader failed", args[0])
            self.assertIn("bad loader", str(args[2]))

    def test_get_raw_content_returns_empty_when_reader_returns_no_docs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "doc.txt"
            file_path.write_text("fallback text", encoding="utf-8")
            self.mock_directory_reader.load_resource.return_value = []

            job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})

            item = IngestionItem(id=f"file://{file_path}", source_ref=file_path)
            result = job.get_raw_content(item)

            self.assertEqual(result, "")

    def test_get_raw_content_returns_empty_on_loader_error_for_missing_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            missing_path = Path(temp_dir) / "missing.txt"
            self.mock_directory_reader.load_resource.side_effect = ValueError("missing file")
            job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})

            item = IngestionItem(id=f"file://{missing_path}", source_ref=missing_path)
            with patch("tasks.directory_ingestion.logger.warning") as mock_warning:
                result = job.get_raw_content(item)

            self.assertEqual(result, "")
            mock_warning.assert_called_once()
            args = mock_warning.call_args[0]
            self.assertEqual(args[1], missing_path)
            self.assertIn("SimpleDirectoryReader failed", args[0])
            self.assertIn("missing file", str(args[2]))

    def test_config_parses_bools_and_num_files_limit_with_forced_raise_on_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            job = DirectoryIngestionJob(
                {
                    "name": "local",
                    "config": {
                        "path": temp_dir,
                        "recursive": False,
                        "exclude_hidden": False,
                        "exclude_empty": True,
                        "raise_on_error": False,
                        "num_files_limit": 7,
                    },
                }
            )

            cfg = job.connector_config
            self.assertEqual(cfg.recursive, False)
            self.assertEqual(cfg.exclude_hidden, False)
            self.assertEqual(cfg.exclude_empty, True)
            self.assertEqual(cfg.num_files_limit, 7)
            # raise_on_error / errors are hardcoded in _build_directory_reader,
            # verified via the SimpleDirectoryReader constructor call
            self.mock_reader_class.assert_called_with(
                input_dir=str(Path(temp_dir).resolve()),
                recursive=False,
                required_exts=None,
                exclude_hidden=False,
                exclude_empty=True,
                num_files_limit=7,
                encoding="utf-8",
                errors="ignore",
                raise_on_error=True,
            )

    def test_config_forces_errors_ignore(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            DirectoryIngestionJob(
                {
                    "name": "local",
                    "config": {
                        "path": temp_dir,
                        "errors": "replace",
                    },
                }
            )

            # errors="replace" from config is dropped (extra="ignore"),
            # _build_directory_reader always passes errors="ignore"
            call_kwargs = self.mock_reader_class.call_args.kwargs
            self.assertEqual(call_kwargs["errors"], "ignore")

    def test_get_item_name_uses_relative_sanitized_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            nested_dir = base / "A folder"
            nested_dir.mkdir()
            file_path = nested_dir / "Angstrom ?.txt"
            file_path.write_text("x", encoding="utf-8")

            job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})
            item = IngestionItem(id=f"file://{file_path}", source_ref=file_path)

            self.assertEqual(job.get_item_name(item), "A_folder_Angstrom_.txt")

    def test_get_item_name_fallback_to_bare_filename_when_path_outside_base(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})
            # Item whose path is outside the configured base (e.g. symlink escape)
            outside_path = Path(temp_dir).resolve().parent / "outside_dir" / "file.txt"
            item = IngestionItem(id=f"file://{outside_path}", source_ref=outside_path)
            # Falls back to bare filename when relative_to raises ValueError
            self.assertEqual(job.get_item_name(item), "file.txt")


class TestDirectoryDiscoveryWithRealReader(unittest.TestCase):
    """Extension filtering belongs to SimpleDirectoryReader, so these tests do not mock it."""

    def _list_names(self, directory: str, **config) -> list[str]:
        job = DirectoryIngestionJob({"name": "local", "config": {"path": directory, **config}})
        return sorted(Path(item.source_ref).name for item in job.list_items())

    def test_video_is_listed_when_required_exts_is_omitted(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            (Path(temp_dir) / "talk.mp4").write_bytes(b"video")
            (Path(temp_dir) / "note.txt").write_text("note", encoding="utf-8")

            self.assertEqual(self._list_names(temp_dir), ["note.txt", "talk.mp4"])

    def test_required_exts_without_video_excludes_it(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            (Path(temp_dir) / "talk.mp4").write_bytes(b"video")
            (Path(temp_dir) / "note.txt").write_text("note", encoding="utf-8")

            self.assertEqual(self._list_names(temp_dir, required_exts="txt,md"), ["note.txt"])

    def test_required_exts_with_video_lists_both(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            (Path(temp_dir) / "talk.mp4").write_bytes(b"video")
            (Path(temp_dir) / "note.txt").write_text("note", encoding="utf-8")
            (Path(temp_dir) / "image.png").write_bytes(b"png")

            self.assertEqual(self._list_names(temp_dir, required_exts="mp4,txt"), ["note.txt", "talk.mp4"])


if __name__ == "__main__":
    unittest.main()
