import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from pydantic import ValidationError

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

    def _make_file_job(self, temp_dir: str, name: str, source: str):
        """Build a directory job whose reader returns the provided file content."""
        file_path = Path(temp_dir) / name
        file_path.write_text(source, encoding="utf-8")
        self.mock_directory_reader.load_resource.return_value = [Mock(text=source)]
        job = DirectoryIngestionJob({"name": "local", "config": {"path": temp_dir}})
        item = IngestionItem(id=f"file://{file_path}", source_ref=file_path)
        return job, item

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

    def test_markdown_frontmatter_is_removed_from_content_and_returned_as_metadata(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = "---\ntitle: Example\ntags:\n  - rag\n---\n# Body\n\nText"
            job, item = self._make_file_job(temp_dir, "doc.md", source)

            content = job.get_raw_content(item)
            extra = job.get_extra_metadata(item, content, {})

            self.assertEqual(content, "# Body\n\nText")
            self.assertEqual(extra, {"md_title": "Example", "md_tags": ["rag"]})

    def test_frontmatter_keeps_inline_and_block_scalar_lists(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = (
                "---\n"
                "tags: [markdown, tutorial, web]\n"
                "categories:\n"
                "  - docs\n"
                "  - rag\n"
                "---\n"
                "Body"
            )
            job, item = self._make_file_job(temp_dir, "doc.md", source)

            extra = job.get_extra_metadata(item, job.get_raw_content(item), {})

            self.assertEqual(extra["md_tags"], ["markdown", "tutorial", "web"])
            self.assertEqual(extra["md_categories"], ["docs", "rag"])

    def test_frontmatter_keeps_special_character_property_names(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = (
                "---\n"
                "og:image: /images/guide.jpg\n"
                "twitter:card: summary_large_image\n"
                "feature-flag: true\n"
                "---\n"
                "Body"
            )
            job, item = self._make_file_job(temp_dir, "doc.md", source)

            extra = job.get_extra_metadata(item, job.get_raw_content(item), {})

            self.assertEqual(extra["md_og:image"], "/images/guide.jpg")
            self.assertEqual(extra["md_twitter:card"], "summary_large_image")
            self.assertIs(extra["md_feature-flag"], True)

    def test_frontmatter_normalizes_dates_to_iso_strings(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = "---\ndate: 2024-01-15\nupdated: 2024-01-15T14:30:00Z\n---\nBody"
            job, item = self._make_file_job(temp_dir, "doc.md", source)

            extra = job.get_extra_metadata(item, job.get_raw_content(item), {})

            self.assertEqual(extra["md_date"], "2024-01-15")
            self.assertEqual(extra["md_updated"], "2024-01-15T14:30:00+00:00")

    def test_frontmatter_discards_structured_properties(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = (
                "---\n"
                "title: Documentation Page\n"
                "tags: [markdown, vuepress, documentation]\n"
                "seo:\n"
                "  title: Nested title\n"
                "meta:\n"
                "  - name: description\n"
                "    content: Page description\n"
                "  - name: keywords\n"
                "    content: markdown vuepress documentation\n"
                "matrix: [[a, b], [c]]\n"
                "---\n"
                "Body"
            )
            job, item = self._make_file_job(temp_dir, "doc.md", source)

            extra = job.get_extra_metadata(item, job.get_raw_content(item), {})

            self.assertEqual(
                extra,
                {
                    "md_title": "Documentation Page",
                    "md_tags": ["markdown", "vuepress", "documentation"],
                },
            )

    def test_markdown_checksum_changes_when_only_frontmatter_changes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = "---\ntitle: Original\n---\nBody"
            job, item = self._make_file_job(temp_dir, "doc.md", source)
            original_checksum = job.get_item_checksum(item)

            Path(item.source_ref).write_text(
                "---\ntitle: Updated\n---\nBody",
                encoding="utf-8",
            )

            self.assertNotEqual(job.get_item_checksum(item), original_checksum)

    def test_markdown_without_frontmatter_is_unchanged(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = "# Body\n\nText"
            job, item = self._make_file_job(temp_dir, "doc.md", source)

            content = job.get_raw_content(item)

            self.assertEqual(content, source)
            self.assertEqual(job.get_extra_metadata(item, content, {}), {})

    def test_malformed_markdown_frontmatter_is_preserved(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = "---\ntitle: [broken\n---\n# Body"
            job, item = self._make_file_job(temp_dir, "doc.md", source)

            content = job.get_raw_content(item)

            self.assertEqual(content, source)
            self.assertEqual(job.get_extra_metadata(item, content, {}), {})

    def test_non_mapping_markdown_frontmatter_is_preserved(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = "---\n- one\n- two\n---\n# Body"
            job, item = self._make_file_job(temp_dir, "doc.md", source)

            content = job.get_raw_content(item)

            self.assertEqual(content, source)
            self.assertEqual(job.get_extra_metadata(item, content, {}), {})

    def test_non_markdown_frontmatter_like_content_is_unchanged(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = "---\ntitle: Example\n---\nText"
            job, item = self._make_file_job(temp_dir, "doc.txt", source)

            content = job.get_raw_content(item)

            self.assertEqual(content, source)
            self.assertEqual(job.get_extra_metadata(item, content, {}), {})

    def test_json_frontmatter_is_not_processed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = '{\n"title": "JSON"\n}\nBody'
            job, item = self._make_file_job(temp_dir, "doc.md", source)

            content = job.get_raw_content(item)

            self.assertEqual(content, source)
            self.assertEqual(job.get_extra_metadata(item, content, {}), {})

    def test_toml_frontmatter_is_not_processed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = '+++\ntitle = "TOML"\n+++\nBody'
            job, item = self._make_file_job(temp_dir, "doc.md", source)

            content = job.get_raw_content(item)

            self.assertEqual(content, source)
            self.assertEqual(job.get_extra_metadata(item, content, {}), {})

    def test_process_item_preserves_reserved_metadata_and_ingests_markdown_body(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = "---\nsource: overridden\ntitle: Example\n---\nBody"
            job, item = self._make_file_job(temp_dir, "doc.md", source)
            job.metadata_tracker = Mock()
            job.vector_manager = Mock()
            job.metadata_tracker.get_latest_record.return_value = None

            result = job.process_item(item)

            document = job.vector_manager.insert_documents.call_args.args[0][0]
            self.assertEqual(result, 1)
            self.assertEqual(document.text, "Body")
            self.assertEqual(document.metadata["source"], "directory")
            self.assertEqual(document.metadata["md_source"], "overridden")
            self.assertEqual(document.metadata["md_title"], "Example")

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


if __name__ == "__main__":
    unittest.main()
