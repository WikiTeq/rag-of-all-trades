import gc
import hashlib
import io
import logging
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from docling.datamodel import asr_model_specs
from docling.datamodel.base_models import ConversionStatus, InputFormat
from docling.datamodel.pipeline_options import VideoPipelineOptions
from docling.datamodel.pipeline_options_asr_model import InlineAsrNativeWhisperOptions
from docling.document_converter import DocumentConverter, VideoFormatOption
from llama_index.core import Document
from markitdown import MarkItDown, StreamInfo

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.helper_classes.ingestion_result import IngestionItemResult, IngestionItemStatus
from tasks.helper_classes.metadata_tracker import MetadataTracker
from tasks.helper_classes.vector_store import VectorStoreManager
from tasks.schemas import BaseMetadataSchema

logger = logging.getLogger(__name__)

DEFAULT_VIDEO_ASR_MODEL = "WHISPER_TINY"


class IngestionJob(ABC):
    """Abstract base class for all ingestion jobs that process content from various sources.

    This class provides the core framework for ingesting content from different data sources
    (files, APIs, databases, etc.) into a vector store for RAG applications. It handles
    duplicate detection, versioning, metadata tracking, and provides hooks for customization.
    """

    # File extensions (lowercase) that connectors send to the Docling video transcription.
    VIDEO_EXTENSIONS = frozenset({".mp4", ".avi", ".mov", ".mkv", ".webm"})

    @property
    def content_format(self) -> str:
        """Content format reported in document metadata. Override in subclasses if needed."""
        return "markdown"

    def __init__(self, config: dict):
        """Initialize the ingestion job with configuration and core components.

        Args:
            config: Dictionary containing job configuration including source name and settings

        Sets up metadata tracking, vector store management, and duplicate detection infrastructure.
        """
        self.config = config
        cfg = config.get("config", None)
        if cfg is None:
            cfg = {}
        if not isinstance(cfg, dict):
            raise ValueError("config.config must be an object")
        raw_delay = cfg.get("request_delay", 0)
        try:
            self.request_delay = float(raw_delay)
        except (TypeError, ValueError) as exc:
            raise ValueError("request_delay must be a number") from exc
        if self.request_delay < 0:
            raise ValueError("request_delay must be >= 0")

        self.video_asr_model = str(cfg.get("video_asr_model") or DEFAULT_VIDEO_ASR_MODEL).strip().upper()
        self._video_asr_options = self._resolve_video_asr_options(self.video_asr_model)

        self.source_name = config.get("name")
        self.metadata_tracker = MetadataTracker()
        self.vector_manager = VectorStoreManager()

        # Seen checksums - prevent reprocessing identical content
        self._seen_capacity = 10000
        self._seen = OrderedDict()

        # Lazy-initialised MarkItDown instance shared across conversion calls
        self._markitdown: MarkItDown | None = None
        # Lazy-initialised Docling converter; loading the ASR model is expensive
        self._video_converter: DocumentConverter | None = None

    def _get_markitdown(self) -> MarkItDown:
        """Return a shared MarkItDown instance, creating it on first use."""
        if self._markitdown is None:
            self._markitdown = MarkItDown()
        return self._markitdown

    @staticmethod
    def _resolve_video_asr_options(name: str) -> InlineAsrNativeWhisperOptions:
        """Return the Docling native Whisper spec named ``name`` (for example ``WHISPER_TINY``).

        Only ``WHISPER_*`` specs that run on the CPU through openai-whisper are valid. The
        MLX, Transformers and S2T specs need other runtimes, so they are rejected.
        """
        spec = getattr(asr_model_specs, name, None) if name.startswith("WHISPER_") else None
        if not isinstance(spec, InlineAsrNativeWhisperOptions):
            valid = sorted(
                attr
                for attr in dir(asr_model_specs)
                if attr.startswith("WHISPER_")
                and isinstance(getattr(asr_model_specs, attr), InlineAsrNativeWhisperOptions)
            )
            raise ValueError(f"video_asr_model must be one of: {', '.join(valid)}")
        return spec

    def _get_video_converter(self) -> DocumentConverter:
        """Return a shared Docling video converter, creating it on first use.

        Transcription only: frame images and speaker diarization are disabled.
        """
        if self._video_converter is None:
            options = VideoPipelineOptions(
                asr_options=self._video_asr_options,
                generate_frame_images=False,
                enable_diarization=False,
            )
            self._video_converter = DocumentConverter(
                allowed_formats=[InputFormat.VIDEO],
                format_options={InputFormat.VIDEO: VideoFormatOption(pipeline_options=options)},
            )
        return self._video_converter

    @staticmethod
    def _format_transcript_timestamp(seconds: float) -> str:
        """Format seconds as ``mm:ss``, or ``h:mm:ss`` from one hour."""
        minutes, secs = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        return f"{hours}:{minutes:02d}:{secs:02d}" if hours else f"{minutes:02d}:{secs:02d}"

    @staticmethod
    def _format_transcript_segment(item) -> str:
        """Return a transcript text item prefixed with its ``[start-end]`` time range."""
        text = item.text.strip()
        if not text or not item.source:
            return text
        track = item.source[0]
        start = IngestionJob._format_transcript_timestamp(track.start_time)
        end = IngestionJob._format_transcript_timestamp(track.end_time)
        return f"[{start}-{end}] {text}"

    def convert_video_to_markdown(self, video_path: Path | str) -> str:
        """Transcribe the audio track of a video file to Markdown using Docling.

        Each transcript segment is a paragraph with a ``[mm:ss-mm:ss]`` time prefix
        (``[h:mm:ss-h:mm:ss]`` past one hour). A video without speech returns an empty string.

        Args:
            video_path: Path to a local video file (.mp4, .avi, .mov, .mkv, .webm).

        Raises:
            RuntimeError: When Docling cannot convert the video. The caller can retry later.
        """
        result = self._get_video_converter().convert(Path(video_path), raises_on_error=False)
        messages = [error.error_message for error in result.errors]
        failed = result.status not in (ConversionStatus.SUCCESS, ConversionStatus.PARTIAL_SUCCESS) or (
            result.status == ConversionStatus.PARTIAL_SUCCESS and messages
        )
        if failed:
            details = f": {'; '.join(messages)}" if messages else ""
            raise RuntimeError(f"Docling video conversion failed for {video_path} ({result.status}){details}")
        return "\n\n".join(self._format_transcript_segment(item) for item in result.document.texts).strip()

    def convert_to_markdown(self, content: bytes | str, fallback: str = "", file_extension: str | None = None) -> str:
        """Convert bytes or text to Markdown using MarkItDown.

        Falls back to ``fallback`` when conversion produces an empty result or
        raises an exception. For str input, falls back to the original string
        when no explicit fallback is provided.

        Args:
            content: Raw bytes or plain-text string to convert.
            fallback: Text to return when conversion yields nothing.
                      Defaults to empty string; for str input defaults to the
                      original string.
            file_extension: Optional file extension hint (e.g. ``".pdf"``) so
                             MarkItDown can pick the right converter for binary
                             content whose type can't be inferred from the bytes.

        Returns:
            Converted Markdown string, or ``fallback`` on failure/empty result.
        """
        if isinstance(content, str):
            if not content.strip():
                return content
            fallback = fallback or content
            content = content.encode("utf-8")
        stream_info = StreamInfo(extension=file_extension) if file_extension else None
        try:
            result = self._get_markitdown().convert_stream(io.BytesIO(content), stream_info=stream_info)
            converted = result.markdown or ""
            if converted.strip():
                return converted
            logger.debug("MarkItDown produced empty result; using fallback text")
            return fallback
        except Exception as exc:
            logger.warning("MarkItDown conversion failed: %s; falling back", exc)
            return fallback

    @property
    @abstractmethod
    def source_type(self) -> str:
        """Return the type identifier for this data source (e.g., 's3', 'mediawiki', 'serpapi')."""
        pass

    @abstractmethod
    def list_items(self) -> Iterable[IngestionItem]:
        """Discover and yield all items that need to be processed from the data source.

        This method should iterate through all available resources in the source and yield
        IngestionItem objects containing metadata about each item. It should
        handle pagination, filtering, and any source-specific discovery logic.

        Yields:
            IngestionItem: Objects containing item ID, source reference, and last modified time
        """
        pass

    @abstractmethod
    def get_raw_content(self, item: IngestionItem) -> str | IngestionItemResult:
        """Fetch and return the raw text content for a given item.

        Args:
            item: The ingestion item to fetch content for

        Returns:
            str: The raw text content of the item. An empty string is treated as ``FAIL``:
                nothing is stored and the item is fetched again on the next run.
            IngestionItemResult: An explicit outcome. ``SUCCESS`` carries the text in
                ``result``; an empty text is stored as a zero-chunk record so an unchanged
                item is not fetched again. ``FAIL`` carries ``errors``; nothing is stored
                and the item is tried again on the next run.
        """
        pass

    @abstractmethod
    def get_item_name(self, item: IngestionItem) -> str:
        """Generate a unique, filesystem-safe name for the item.

        Args:
            item: The ingestion item to generate a name for

        Returns:
            str: A sanitized filename that uniquely identifies this item
        """
        pass

    def get_item_checksum(self, item: IngestionItem) -> str | None:
        """Return a pre-computed checksum or revision ID for the item, or None to fall back.

        Override this in subclasses when the source provides a stable identifier
        (e.g. a revision ID, ETag, or content hash) that can be used to detect changes
        *before* fetching the full item content.

        If this method returns a non-None value it is compared against the stored checksum
        and content fetching is skipped entirely when they match.

        Args:
            item: The ingestion item to return a checksum for

        Returns:
            str | None: A revision string or identifier, or None to use content-based MD5
        """
        return None

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        """Hook for subclasses to provide additional metadata.

        Default implementation returns an empty dictionary. Subclasses can override
        this to add source-specific fields (e.g., URLs, tags, etc.) without
        needing to construct the standard metadata dictionary. Keys that match
        Keys matching BaseMetadataSchema fields (source, key, checksum, version, format,
        source_name, file_name, last_modified) are ignored and will not overwrite
        standard metadata.

        Args:
            item: The ingestion item being processed
            content: The raw text content of the item
            metadata: The standard metadata dictionary constructed by process_item

        Returns:
            dict: Additional metadata to be merged into the final document metadata
        """
        return {}

    def _seen_add(self, checksum: str) -> bool:
        """Track content checksums to prevent reprocessing of identical content.

        Uses an LRU cache approach with OrderedDict to maintain a bounded set of
        recently seen checksums. This prevents memory growth while avoiding
        duplicate processing within a reasonable time window.

        Args:
            checksum: Checksum or revision ID to track

        Returns:
            bool: True if this is new content, False if already seen recently
        """
        if checksum in self._seen:
            self._seen.move_to_end(checksum)
            return False
        self._seen[checksum] = True
        if len(self._seen) > self._seen_capacity:
            self._seen.popitem(last=False)
        return True

    @staticmethod
    def _normalize_content(fetched: str | IngestionItemResult) -> IngestionItemResult:
        """Make sure the return value of ``get_raw_content`` is an ``IngestionItemResult``.

        An ``IngestionItemResult`` is returned as is. A legacy string with text becomes
        ``SUCCESS``. A legacy empty (or whitespace-only) string becomes ``FAIL``, because
        connectors use it to report a failed fetch.
        """
        if isinstance(fetched, IngestionItemResult):
            return fetched
        if fetched.strip():
            return IngestionItemResult(status=IngestionItemStatus.SUCCESS, result=fetched)
        return IngestionItemResult(status=IngestionItemStatus.FAIL, errors=("empty content",))

    def _fetch_content(self, item: IngestionItem) -> str | None:
        """Fetch the content of ``item``. Return None, after logging the errors, on ``FAIL``."""
        result = self._normalize_content(self.get_raw_content(item))
        if result.status is IngestionItemStatus.FAIL:
            logger.error("Failed to fetch content for item %s: %s", item.id, "; ".join(result.errors) or "no details")
            return None
        return result.result

    def _record_empty_item(self, item: IngestionItem, item_name: str, checksum: str, latest) -> None:
        """Store a zero-chunk record so an unchanged empty item is not fetched again.

        Only used for a successful fetch with empty content (``IngestionItemResult``).
        Without the record, every run would repeat an expensive fetch such as video
        transcription. Previous embeddings are deleted: the content is now empty.
        """
        if latest:
            self.metadata_tracker.delete_previous_embeddings(item_name)
        self.metadata_tracker.record_metadata(
            item_name,
            checksum,
            (latest.version + 1) if latest else 1,
            0,
            item.last_modified or datetime.now(UTC),
            extra_metadata={"source_name": self.source_name},
        )

    def process_item(self, item: IngestionItem):
        """Process a single ingestion item through the complete pipeline.

        This method orchestrates the entire ingestion workflow for one item:
        1. Resolve checksum — either from get_item_checksum() or by fetching content and computing MD5
        2. Skip if checksum matches the stored record (unchanged) or was already seen this run
        3. Handle versioning and cleanup of old embeddings
        4. Create document with metadata
        5. Store in vector database and update metadata tracking

        Args:
            item: The ingestion item to process

        Returns:
            int: 1 if item was successfully ingested, 0 if skipped or failed
        """
        try:
            pre_checksum = self.get_item_checksum(item)
            raw_content = None

            if pre_checksum:
                # Fast path: resolve checksum without fetching content
                new_checksum = pre_checksum
            else:
                # Standard path: fetch content and compute MD5
                raw_content = self._fetch_content(item)
                if raw_content is None:
                    return 0
                new_checksum = hashlib.md5(raw_content.encode("utf-8"), usedforsecurity=False).hexdigest()

            item_name = self.get_item_name(item)

            # Unified dedup checks
            latest = self.metadata_tracker.get_latest_record(item_name)
            if latest and latest.checksum == new_checksum:
                logger.info(f"Skipping unchanged item: {item_name}")
                return 0
            seen_key = f"{item.id}:{new_checksum}"
            if not self._seen_add(seen_key):
                logger.info(f"Skipping duplicate checksum for item: {item.id}")
                return 0

            # Fetch content for the fast path only after dedup checks pass —
            # avoids the expensive API call when the item is unchanged or already seen.
            if raw_content is None:
                raw_content = self._fetch_content(item)
                if raw_content is None:
                    # A failed fetch must stay retryable, also within this job.
                    self._seen.pop(seen_key, None)
                    return 0

            if not raw_content.strip():
                # Only an explicit SUCCESS result reaches this point with empty content.
                logger.info(f"Recording empty content for item: {item.id}")
                self._record_empty_item(item, item_name, new_checksum, latest)
                return 0

            if latest:
                logger.info(f"Updating item {item_name} from version {latest.version}")
                self.metadata_tracker.delete_previous_embeddings(item_name)

            version = (latest.version + 1) if latest else 1

            last_modified_ts = item.last_modified or datetime.now(UTC)

            # Standard metadata (reserved keys must not be overwritten by get_extra_metadata)
            metadata = BaseMetadataSchema(
                source=self.source_type,
                key=item_name,
                checksum=new_checksum,
                version=version,
                format=self.content_format,
                source_name=self.source_name,
                file_name=item_name,
                last_modified=str(last_modified_ts),
            ).model_dump()

            extra = self.get_extra_metadata(item, raw_content, metadata)
            filtered_extra = {k: v for k, v in extra.items() if k not in BaseMetadataSchema.model_fields}
            metadata.update(filtered_extra)

            doc = Document(text=raw_content, metadata=metadata)

            self.vector_manager.insert_documents([doc])

            self.metadata_tracker.record_metadata(
                item_name,
                new_checksum,
                version,
                1,
                last_modified_ts,
                extra_metadata={"source_name": self.source_name},
            )

            logger.info(f"Successfully ingested: {item_name} (version {version})")

            gc.collect()

            return 1

        except Exception:
            logger.exception(f"Failed to process item {item}")
            return 0

    def run(self):
        """Execute the complete ingestion job for this data source.

        Discovers all items using list_items(), processes each one through process_item(),
        and provides comprehensive progress tracking and error reporting. Continues
        processing even if individual items fail — but a fatal error while discovering
        items (list_items() itself raising, e.g. an auth or listing-API failure) is not
        swallowed: it propagates so the Celery task is marked FAILED instead of silently
        "succeeding" with an error logged nobody reads (the task result is discarded via
        ignore_result=True).

        Returns:
            str: Summary message indicating total items processed and skipped

        Raises:
            Exception: Any exception raised while iterating list_items() or otherwise
                outside process_item()'s own per-item error handling.
        """
        total = 0
        skipped = 0

        logger.info(f"[{self.source_name}] Starting ingestion job")

        for item in self.list_items():
            count = self.process_item(item)
            if count == 0:
                skipped += 1
                continue

            total += count
            if self.request_delay > 0:
                time.sleep(self.request_delay)

        result_msg = f"[{self.source_name}] Completed: {total} ingested, {skipped} skipped"
        logger.info(result_msg)
        return result_msg
