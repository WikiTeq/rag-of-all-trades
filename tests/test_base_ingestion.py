import hashlib
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from docling.datamodel import asr_model_specs
from docling.datamodel.base_models import ConversionStatus, InputFormat

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.helper_classes.ingestion_result import IngestionItemResult, IngestionItemStatus
from tasks.schemas import BaseMetadataSchema


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
        yield from self._items

    def get_raw_content(self, item):
        return self._content_by_id.get(item.id, "")

    def get_item_name(self, item):
        return self._name_by_id.get(item.id, item.id)


@pytest.fixture
def base_config():
    return {"name": "test-source"}


class TestIngestionJob:
    def test_standard_metadata_construction(self, base_config):
        """Standard metadata should be built correctly in process_item."""
        job = DummyIngestionJob(base_config)
        item = IngestionItem(
            id="item-1",
            source_ref="src",
            last_modified=datetime(2024, 1, 1),
        )
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.get_raw_content = Mock(return_value="content")
        job.metadata_tracker.get_latest_record.return_value = None

        job.process_item(item)

        # Verify metadata passed to VectorStore
        args, _ = job.vector_manager.insert_documents.call_args
        metadata = args[0][0].metadata
        assert metadata["source"] == "dummy"
        assert metadata["key"] == "item-1"
        assert metadata["version"] == 1
        assert metadata["last_modified"] == "2024-01-01 00:00:00"

    def test_get_extra_metadata_merge(self, base_config):
        """Extra metadata from hook should be merged into final result."""
        job = DummyIngestionJob(base_config)
        job.get_extra_metadata = Mock(return_value={"custom": "val"})

        item = IngestionItem(id="item-1", source_ref="src")
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.get_raw_content = Mock(return_value="content")
        job.metadata_tracker.get_latest_record.return_value = None

        job.process_item(item)

        args, _ = job.vector_manager.insert_documents.call_args
        metadata = args[0][0].metadata
        assert metadata["custom"] == "val"
        assert metadata["source"] == "dummy"  # Standard still there

    def test_get_extra_metadata_reserved_keys_not_overwritten(self, base_config):
        """Extra metadata must not overwrite reserved standard keys."""
        schema_fields = BaseMetadataSchema.model_fields

        job = DummyIngestionJob(base_config)
        job.get_extra_metadata = Mock(return_value={k: "overwrite" for k in schema_fields})

        item = IngestionItem(id="item-1", source_ref="src")
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.get_raw_content = Mock(return_value="content")
        job.metadata_tracker.get_latest_record.return_value = None

        job.process_item(item)

        args, _ = job.vector_manager.insert_documents.call_args
        metadata = args[0][0].metadata
        for key in schema_fields:
            assert metadata[key] != "overwrite", f"Reserved key {key} was overwritten"
        assert metadata["source"] == "dummy"
        assert metadata["key"] == "item-1"
        assert metadata["version"] == 1

    def test_seen_add_lru_eviction(self, base_config):
        job = DummyIngestionJob(base_config)
        job._seen_capacity = 2

        assert job._seen_add("a") is True
        assert job._seen_add("b") is True
        assert job._seen_add("a") is False
        assert job._seen_add("c") is True
        assert job._seen_add("b") is True

    def test_process_item_skips_empty_content(self, base_config):
        item = IngestionItem(id="item-1", source_ref="src")
        job = DummyIngestionJob(
            base_config,
            items=[item],
            content_by_id={"item-1": "   "},
        )
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()

        result = job.process_item(item)

        assert result == 0
        job.metadata_tracker.get_latest_record.assert_not_called()
        job.vector_manager.insert_documents.assert_not_called()

    def test_process_item_skips_unchanged_content(self, base_config):
        content = "same content"
        checksum = hashlib.md5(content.encode("utf-8"), usedforsecurity=False).hexdigest()
        item = IngestionItem(id="item-1", source_ref="src")
        job = DummyIngestionJob(
            base_config,
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

        assert result == 0
        job.metadata_tracker.get_latest_record.assert_called_once_with("item-1")
        job.metadata_tracker.delete_previous_embeddings.assert_not_called()
        job.metadata_tracker.record_metadata.assert_not_called()
        job.vector_manager.insert_documents.assert_not_called()

    @patch("tasks.base.Document")
    def test_process_item_updates_version_and_records_metadata(self, mock_document, base_config):
        content = "updated content"
        checksum = hashlib.md5(content.encode("utf-8"), usedforsecurity=False).hexdigest()
        last_modified = datetime(2024, 1, 2, 3, 4, 5)
        item = IngestionItem(
            id="item-1",
            source_ref="src",
            last_modified=last_modified,
        )
        job = DummyIngestionJob(
            base_config,
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

        assert result == 1
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

        assert mock_document.call_count == 1
        _, kwargs = mock_document.call_args
        assert kwargs["text"] == content
        assert kwargs["metadata"]["checksum"] == checksum
        assert kwargs["metadata"]["version"] == 3
        assert kwargs["metadata"]["source"] == "dummy"

    def test_get_item_checksum_default_returns_none(self, base_config):
        job = DummyIngestionJob(base_config)
        item = IngestionItem(id="item-1", source_ref="src")
        assert job.get_item_checksum(item) is None

    def test_process_item_skips_when_pre_checksum_matches(self, base_config):
        """Pre-checksum matches DB record → get_raw_content is never called."""
        item = IngestionItem(id="item-1", source_ref="src")
        job = DummyIngestionJob(base_config, items=[item], content_by_id={"item-1": "content"})
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.metadata_tracker.get_latest_record.return_value = Mock(checksum="rev-42", version=1)

        with (
            patch.object(job, "get_item_checksum", return_value="rev-42"),
            patch.object(job, "get_raw_content") as mock_fetch,
        ):
            result = job.process_item(item)

        assert result == 0
        mock_fetch.assert_not_called()
        job.vector_manager.insert_documents.assert_not_called()

    @patch("tasks.base.Document")
    def test_process_item_stores_when_pre_checksum_differs(self, mock_document, base_config):
        """Pre-checksum differs from DB → content fetched, stored checksum is pre-checksum."""
        content = "new content"
        last_modified = datetime(2024, 6, 1)
        item = IngestionItem(id="item-1", source_ref="src", last_modified=last_modified)
        job = DummyIngestionJob(base_config, items=[item], content_by_id={"item-1": content})
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.metadata_tracker.get_latest_record.return_value = Mock(checksum="rev-41", version=1)

        with (
            patch.object(job, "get_item_checksum", return_value="rev-42"),
            patch.object(job, "_seen_add", return_value=True),
        ):
            result = job.process_item(item)

        assert result == 1
        job.vector_manager.insert_documents.assert_called_once()
        job.metadata_tracker.record_metadata.assert_called_once_with(
            "item-1",
            "rev-42",  # stored checksum is the pre-computed one, not MD5
            2,
            1,
            last_modified,
            extra_metadata={"source_name": "test-source"},
        )
        _, kwargs = mock_document.call_args
        assert kwargs["metadata"]["checksum"] == "rev-42"

    def test_process_item_skips_when_seen_add_returns_false(self, base_config):
        """_seen_add returns False (duplicate checksum this run) → item skipped, content never fetched."""
        item = IngestionItem(id="item-1", source_ref="src")
        job = DummyIngestionJob(base_config, items=[item], content_by_id={"item-1": "content"})
        job.metadata_tracker = Mock()
        job.vector_manager = Mock()
        job.metadata_tracker.get_latest_record.return_value = Mock(checksum="rev-old", version=1)

        with (
            patch.object(job, "get_item_checksum", return_value="rev-42"),
            patch.object(job, "_seen_add", return_value=False),
            patch.object(job, "get_raw_content") as mock_fetch,
        ):
            result = job.process_item(item)

        assert result == 0
        mock_fetch.assert_not_called()
        job.vector_manager.insert_documents.assert_not_called()

    def test_run_reports_totals(self, base_config):
        item1 = IngestionItem(id="item-1", source_ref="src")
        item2 = IngestionItem(id="item-2", source_ref="src")
        job = DummyIngestionJob(base_config, items=[item1, item2])
        job.process_item = Mock(side_effect=[1, 0])

        result = job.run()

        assert result == "[test-source] Completed: 1 ingested, 1 skipped"
        assert job.process_item.call_count == 2


class TestIngestionJobRunFatalErrors(unittest.TestCase):
    """run() must propagate fatal errors from list_items() instead of swallowing them.

    Written as unittest.TestCase (not the pytest-fixture style above) so it actually
    executes under `python -m unittest discover` per the project's test runner.
    """

    def test_run_reraises_list_items_exception(self):
        class FailingListItemsJob(IngestionJob):
            @property
            def source_type(self) -> str:
                return "dummy"

            def list_items(self):
                raise ConnectionError("auth failed")
                yield  # pragma: no cover — makes this a generator function

            def get_raw_content(self, item):
                return ""

            def get_item_name(self, item):
                return item.id

        job = FailingListItemsJob({"name": "test-source"})

        with self.assertRaises(ConnectionError):
            job.run()

    def test_run_does_not_swallow_fatal_error_into_return_string(self):
        """A fatal list_items() error must not be caught and turned into a result string —
        Celery's ignore_result=True means a returned string is silently discarded, so a
        caught-and-stringified error looks identical to success."""

        class FailingListItemsJob(IngestionJob):
            @property
            def source_type(self) -> str:
                return "dummy"

            def list_items(self):
                raise RuntimeError("listing API is down")
                yield  # pragma: no cover — makes this a generator function

            def get_raw_content(self, item):
                return ""

            def get_item_name(self, item):
                return item.id

        job = FailingListItemsJob({"name": "test-source"})

        try:
            job.run()
            self.fail("run() should have raised RuntimeError, not returned normally")
        except RuntimeError as exc:
            self.assertEqual(str(exc), "listing API is down")


class TestIngestionJobMarkdownConversion:
    def test_convert_bytes_returns_converted_text(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.return_value = Mock(markdown="# Hello")
        job._markitdown = mock_md

        result = job.convert_to_markdown(b"raw bytes")

        assert result == "# Hello"
        mock_md.convert_stream.assert_called_once()

    def test_convert_bytes_passes_file_extension_hint(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.return_value = Mock(markdown="# Hello")
        job._markitdown = mock_md

        job.convert_to_markdown(b"raw bytes", file_extension=".pdf")

        _, kwargs = mock_md.convert_stream.call_args
        assert kwargs["stream_info"].extension == ".pdf"

    def test_convert_bytes_without_file_extension_passes_no_stream_info(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.return_value = Mock(markdown="# Hello")
        job._markitdown = mock_md

        job.convert_to_markdown(b"raw bytes")

        _, kwargs = mock_md.convert_stream.call_args
        assert kwargs["stream_info"] is None

    def test_convert_bytes_falls_back_on_empty_result(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.return_value = Mock(markdown="   ")
        job._markitdown = mock_md

        result = job.convert_to_markdown(b"raw", fallback="raw text")

        assert result == "raw text"

    def test_convert_bytes_falls_back_on_exception(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.side_effect = RuntimeError("boom")
        job._markitdown = mock_md

        result = job.convert_to_markdown(b"raw", fallback="fallback")

        assert result == "fallback"

    def test_convert_bytes_default_fallback_is_empty_string(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.return_value = Mock(markdown="")
        job._markitdown = mock_md

        result = job.convert_to_markdown(b"raw")

        assert result == ""

    def test_convert_text_returns_converted_text(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.return_value = Mock(markdown="# Heading")
        job._markitdown = mock_md

        result = job.convert_to_markdown("some wiki text")

        assert result == "# Heading"

    def test_convert_text_falls_back_on_empty_result(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.return_value = Mock(markdown="   ")
        job._markitdown = mock_md

        result = job.convert_to_markdown("original text")

        assert result == "original text"

    def test_convert_text_falls_back_on_exception(self):
        job = DummyIngestionJob({"name": "test-source"})
        mock_md = Mock()
        mock_md.convert_stream.side_effect = RuntimeError("oops")
        job._markitdown = mock_md

        result = job.convert_to_markdown("original text")

        assert result == "original text"

    def test_convert_text_returns_empty_string_unchanged(self):
        job = DummyIngestionJob({"name": "test-source"})
        assert job.convert_to_markdown("") == ""

    def test_convert_text_returns_whitespace_only_unchanged(self):
        job = DummyIngestionJob({"name": "test-source"})
        assert job.convert_to_markdown("   ") == "   "

    def test_get_markitdown_is_lazily_created(self):
        job = DummyIngestionJob({"name": "test-source"})
        assert job._markitdown is None
        with patch("tasks.base.MarkItDown") as mock_cls:
            instance = job._get_markitdown()
            mock_cls.assert_called_once_with()
            assert instance is mock_cls.return_value

    def test_get_markitdown_returns_same_instance(self):
        job = DummyIngestionJob({"name": "test-source"})
        with patch("tasks.base.MarkItDown"):
            first = job._get_markitdown()
            second = job._get_markitdown()
            assert first is second


class TestIngestionJobVideoConversion:
    @staticmethod
    def _segment(text, start, end):
        return Mock(text=text, source=[Mock(start_time=start, end_time=end)])

    @classmethod
    def _job_with_result(cls, status, segments=None, errors=()):
        if segments is None:
            segments = [cls._segment("Hello", 0.0, 4.9)]
        job = DummyIngestionJob({"name": "test-source"})
        converter = Mock()
        converter.convert.return_value = Mock(
            status=status,
            document=Mock(texts=segments),
            errors=[Mock(error_message=message) for message in errors],
        )
        job._video_converter = converter
        return job, converter

    def test_returns_transcript_markdown(self):
        job, converter = self._job_with_result(ConversionStatus.SUCCESS)

        assert job.convert_video_to_markdown("/tmp/a.mp4") == "[00:00-00:04] Hello"
        converter.convert.assert_called_once_with(Path("/tmp/a.mp4"), raises_on_error=False)

    def test_partial_success_with_errors_raises_retryable_error(self):
        job, _ = self._job_with_result(ConversionStatus.PARTIAL_SUCCESS, errors=("audio track cut off", "bad frame"))

        with pytest.raises(RuntimeError, match="audio track cut off; bad frame"):
            job.convert_video_to_markdown("/tmp/a.mp4")

    def test_partial_success_without_errors_does_not_warn(self, caplog):
        # Docling reports a video without speech as a partial success with no error items.
        job, _ = self._job_with_result(ConversionStatus.PARTIAL_SUCCESS, segments=[])

        with caplog.at_level("WARNING", logger="tasks.base"):
            transcript = job.convert_video_to_markdown("/tmp/a.mp4")

        assert transcript == ""
        assert caplog.text == ""

    def test_success_does_not_log_errors(self, caplog):
        job, _ = self._job_with_result(ConversionStatus.SUCCESS)

        with caplog.at_level("WARNING", logger="tasks.base"):
            job.convert_video_to_markdown("/tmp/a.mp4")

        assert "error(s)" not in caplog.text

    def test_segments_are_separate_paragraphs_with_hour_timestamps(self):
        segments = [self._segment("First", 61.2, 65.0), self._segment("Second", 3725.0, 3730.9)]
        job, _ = self._job_with_result(ConversionStatus.SUCCESS, segments)

        assert job.convert_video_to_markdown("/tmp/a.mp4") == "[01:01-01:05] First\n\n[1:02:05-1:02:10] Second"

    def test_segment_without_source_keeps_text_only(self):
        segments = [Mock(text=" Plain ", source=[])]
        job, _ = self._job_with_result(ConversionStatus.SUCCESS, segments)

        assert job.convert_video_to_markdown("/tmp/a.mp4") == "Plain"

    def test_failure_status_raises(self):
        job, _ = self._job_with_result(ConversionStatus.FAILURE)

        with pytest.raises(RuntimeError, match="a.mp4"):
            job.convert_video_to_markdown("/tmp/a.mp4")

    def test_failure_status_includes_conversion_errors(self):
        job, _ = self._job_with_result(ConversionStatus.FAILURE, errors=("no audio stream", "decoder crashed"))

        with pytest.raises(RuntimeError, match="no audio stream; decoder crashed"):
            job.convert_video_to_markdown("/tmp/a.mp4")

    def test_converter_exception_propagates(self):
        job, converter = self._job_with_result(ConversionStatus.SUCCESS)
        converter.convert.side_effect = OSError("ffmpeg missing")

        with pytest.raises(OSError, match="ffmpeg missing"):
            job.convert_video_to_markdown("/tmp/a.mp4")

    def test_video_without_speech_returns_empty_string(self):
        job, _ = self._job_with_result(ConversionStatus.SUCCESS, [])

        assert job.convert_video_to_markdown("/tmp/a.mp4") == ""

    def test_converter_is_lazily_created_for_transcription_only(self):
        job = DummyIngestionJob({"name": "test-source"})
        assert job._video_converter is None
        with (
            patch("tasks.base.DocumentConverter") as mock_cls,
            patch("tasks.base.VideoFormatOption") as mock_option,
            patch("tasks.base.VideoPipelineOptions") as mock_options,
        ):
            first = job._get_video_converter()
            second = job._get_video_converter()

        assert first is second is mock_cls.return_value
        mock_options.assert_called_once_with(
            asr_options=asr_model_specs.WHISPER_TINY, generate_frame_images=False, enable_diarization=False
        )
        mock_option.assert_called_once_with(pipeline_options=mock_options.return_value)
        mock_cls.assert_called_once_with(
            allowed_formats=[InputFormat.VIDEO],
            format_options={InputFormat.VIDEO: mock_option.return_value},
        )

    def test_video_asr_model_defaults_to_whisper_tiny(self):
        job = DummyIngestionJob({"name": "test-source"})

        assert job.video_asr_model == "WHISPER_TINY"
        assert job._video_asr_options is asr_model_specs.WHISPER_TINY

    def test_video_asr_model_selects_docling_spec(self):
        job = DummyIngestionJob({"name": "test-source", "config": {"video_asr_model": " whisper_small "}})
        assert job.video_asr_model == "WHISPER_SMALL"
        with (
            patch("tasks.base.DocumentConverter"),
            patch("tasks.base.VideoFormatOption"),
            patch("tasks.base.VideoPipelineOptions") as mock_options,
        ):
            job._get_video_converter()

        assert mock_options.call_args.kwargs["asr_options"] is asr_model_specs.WHISPER_SMALL

    @pytest.mark.parametrize(
        "name",
        [
            "tiny",
            "huge",
            "logging",
            "WHISPER_TINY_MLX",
            "WHISPER_TINY_S2T",
            "WHISPER_TINY_TRANSFORMERS",
            "GRANITE_4_0_1B_SPEECH",
        ],
    )
    def test_invalid_video_asr_model_is_rejected(self, name):
        with pytest.raises(ValueError, match="video_asr_model must be one of: .*WHISPER_TINY"):
            DummyIngestionJob({"name": "test-source", "config": {"video_asr_model": name}})

    def test_video_extensions_are_defined_on_the_base_class(self):
        assert IngestionJob.VIDEO_EXTENSIONS == frozenset({".mp4", ".avi", ".mov", ".mkv", ".webm"})

    def test_transcript_timestamps_use_minutes_and_hours(self):
        assert IngestionJob._format_transcript_timestamp(65.9) == "01:05"
        assert IngestionJob._format_transcript_timestamp(3725.0) == "1:02:05"


class TestIngestionJobItemResult:
    @staticmethod
    def _job(fetched, pre_checksum="sum-1", latest=None):
        item = IngestionItem(id="item-1", source_ref="src", last_modified=datetime(2026, 1, 2))
        job = DummyIngestionJob({"name": "test-source"}, items=[item])
        job.get_item_checksum = Mock(return_value=pre_checksum)
        job.get_raw_content = (
            Mock(side_effect=fetched) if isinstance(fetched, Exception) else Mock(return_value=fetched)
        )
        job.metadata_tracker = Mock()
        job.metadata_tracker.get_latest_record.return_value = latest
        job.vector_manager = Mock()
        return job, item

    @staticmethod
    def _success(text):
        return IngestionItemResult(IngestionItemStatus.SUCCESS, text)

    @staticmethod
    def _fail(*errors):
        return IngestionItemResult(IngestionItemStatus.FAIL, errors=errors)

    def test_normalize_returns_result_as_is(self):
        for result in (self._success("text"), self._success(""), self._fail("boom")):
            assert IngestionJob._normalize_content(result) is result

    def test_normalize_wraps_legacy_text_as_success(self):
        result = IngestionJob._normalize_content("text")

        assert result == self._success("text")

    @pytest.mark.parametrize("legacy", ["", "  \n"])
    def test_normalize_wraps_legacy_empty_string_as_fail(self, legacy):
        result = IngestionJob._normalize_content(legacy)

        assert result.status is IngestionItemStatus.FAIL
        assert result.errors

    @pytest.mark.parametrize("pre_checksum", ["sum-1", None])
    def test_legacy_empty_string_is_skipped_and_not_recorded(self, pre_checksum):
        job, item = self._job(" ", pre_checksum)

        assert job.process_item(item) == 0

        job.metadata_tracker.record_metadata.assert_not_called()
        job.vector_manager.insert_documents.assert_not_called()

    @pytest.mark.parametrize("pre_checksum", ["sum-1", None])
    def test_legacy_exception_is_not_recorded(self, pre_checksum):
        job, item = self._job(RuntimeError("boom"), pre_checksum)

        assert job.process_item(item) == 0

        job.metadata_tracker.record_metadata.assert_not_called()

    @pytest.mark.parametrize("pre_checksum", ["sum-1", None])
    def test_success_with_text_is_ingested(self, pre_checksum):
        job, item = self._job(self._success("transcript"), pre_checksum)

        assert job.process_item(item) == 1

        document = job.vector_manager.insert_documents.call_args.args[0][0]
        assert document.text == "transcript"
        assert job.metadata_tracker.record_metadata.call_args.args[3] == 1

    def test_success_with_empty_text_is_recorded_as_zero_chunks(self):
        job, item = self._job(self._success(""))

        assert job.process_item(item) == 0

        job.metadata_tracker.record_metadata.assert_called_once_with(
            "item-1", "sum-1", 1, 0, datetime(2026, 1, 2), extra_metadata={"source_name": "test-source"}
        )
        job.metadata_tracker.delete_previous_embeddings.assert_not_called()
        job.vector_manager.insert_documents.assert_not_called()

    def test_success_with_whitespace_text_without_checksum_is_recorded_with_content_checksum(self):
        job, item = self._job(self._success("  \n"), pre_checksum=None)

        assert job.process_item(item) == 0

        checksum = hashlib.md5(b"  \n", usedforsecurity=False).hexdigest()
        assert job.metadata_tracker.record_metadata.call_args.args[:4] == ("item-1", checksum, 1, 0)

    def test_recorded_empty_item_is_not_fetched_again(self):
        job, item = self._job(self._success(""), latest=Mock(checksum="sum-1", version=1))

        assert job.process_item(item) == 0

        job.get_raw_content.assert_not_called()
        job.metadata_tracker.record_metadata.assert_not_called()

    def test_content_that_became_empty_replaces_previous_version(self):
        job, item = self._job(self._success(""), latest=Mock(checksum="old", version=3))

        assert job.process_item(item) == 0

        job.metadata_tracker.delete_previous_embeddings.assert_called_once_with("item-1")
        assert job.metadata_tracker.record_metadata.call_args.args[:4] == ("item-1", "sum-1", 4, 0)

    @pytest.mark.parametrize("pre_checksum", ["sum-1", None])
    def test_failure_keeps_previous_record_and_logs_errors(self, pre_checksum, caplog):
        job, item = self._job(self._fail("ffmpeg missing", "bad stream"), pre_checksum, latest=Mock(checksum="old"))

        with caplog.at_level("ERROR"):
            assert job.process_item(item) == 0

        assert "ffmpeg missing; bad stream" in caplog.text
        job.metadata_tracker.delete_previous_embeddings.assert_not_called()
        job.metadata_tracker.record_metadata.assert_not_called()
        job.vector_manager.insert_documents.assert_not_called()

    def test_failure_is_retried_in_the_same_job(self):
        job, item = self._job(self._fail("boom"))

        assert job.process_item(item) == 0

        job.get_raw_content.return_value = self._success("transcript")
        assert job.process_item(item) == 1
