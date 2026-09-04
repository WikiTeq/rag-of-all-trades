import gc
import hashlib
import io
import logging
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from llama_index.core import Document
from markitdown import MarkItDown, StreamInfo

from tasks.helper_classes.ingestion_item import IngestionItem
from tasks.helper_classes.metadata_tracker import MetadataTracker
from tasks.helper_classes.vector_store import VectorStoreManager
from tasks.schemas import BaseMetadataSchema
from utils.config import settings

logger = logging.getLogger(__name__)


class IngestionJob(ABC):
    """Abstract base class for all ingestion jobs that process content from various sources.

    This class provides the core framework for ingesting content from different data sources
    (files, APIs, databases, etc.) into a vector store for RAG applications. It handles
    duplicate detection, versioning, metadata tracking, and provides hooks for customization.
    """

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

        raw_acl_owner = cfg.get("acl_owner")
        self.acl_owner = str(raw_acl_owner).strip().lower() if raw_acl_owner else None
        self.acl_enabled = settings.env.ENABLE_ACL

        self.source_name = config.get("name")
        self.metadata_tracker = MetadataTracker()
        self.vector_manager = VectorStoreManager()

        # Seen checksums - prevent reprocessing identical content
        self._seen_capacity = 10000
        self._seen = OrderedDict()

        # Lazy-initialised MarkItDown instance shared across conversion calls
        self._markitdown: MarkItDown | None = None

    def _get_markitdown(self) -> MarkItDown:
        """Return a shared MarkItDown instance, creating it on first use."""
        if self._markitdown is None:
            self._markitdown = MarkItDown()
        return self._markitdown

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
    def get_raw_content(self, item: IngestionItem) -> str:
        """Fetch and return the raw text content for a given item.

        Args:
            item: The ingestion item to fetch content for

        Returns:
            str: The raw text content of the item
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

    def get_acl_list(self, item: IngestionItem) -> list[str]:
        """Hook for subclasses to resolve the access control list for an item.

        Default implementation returns an empty list — a document with no ACL
        is fully private (no one has access). Override in subclasses to resolve
        and return the flat list of email identities that should have access,
        or ``['*']`` for a publicly accessible document.

        This is deliberately separate from get_extra_metadata(): ACL data goes
        through this dedicated hook so the base class can apply ACL-specific
        handling (fail-closed on error, acl_owner fallback, exclusion from LLM/
        embedding metadata) that doesn't apply to arbitrary extra metadata.

        Only consulted when ACL support is enabled (``ENABLE_ACL``); ignored
        entirely otherwise.

        Args:
            item: The ingestion item to resolve the ACL for

        Returns:
            list[str]: Email identities with access, or ['*'] for public.
                       Empty list means the document is private.
        """
        return []

    def _sanitize_acl_list(self, acl: list[str], *, item_id: str) -> list[str]:
        """Trim, lowercase, de-duplicate and sort an ACL list for storage.

        A list containing both '*' and specific emails is ambiguous (the
        ticket's own rules treat '*' and emails as mutually exclusive), so it
        is collapsed to ['*'] — public wins — with a warning logged so the
        connector bug producing the mixed list is visible.

        Args:
            acl: Raw ACL entries as returned by get_acl_list()
            item_id: Item identifier, used only for the warning message

        Returns:
            list[str]: Sanitized, sorted ACL list.
        """
        normalized = {str(entry).strip().lower() for entry in acl if str(entry).strip()}
        if "*" in normalized and len(normalized) > 1:
            logger.warning(
                f"[{self.source_name}] ACL for item {item_id} mixes '*' with explicit emails; treating as public"
            )
            return ["*"]
        return sorted(normalized)

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
                raw_content = self.get_raw_content(item)
                if not raw_content.strip():
                    logger.warning(f"Skipping empty content for item: {item.id}")
                    return 0
                new_checksum = hashlib.md5(raw_content.encode("utf-8"), usedforsecurity=False).hexdigest()

            item_name = self.get_item_name(item)

            # Resolve and sanitize the ACL before the dedup decision, since an
            # ACL-only change (content unchanged, access changed) must still
            # trigger reprocessing. Ignored entirely when ACL support is off.
            acl_list: list[str] = []
            if self.acl_enabled:
                try:
                    raw_acl = self.get_acl_list(item)
                    acl_failed = False
                except Exception:
                    logger.exception(f"get_acl_list failed for item {item.id}; treating as empty (fail-closed)")
                    raw_acl = []
                    acl_failed = True

                acl_list = self._sanitize_acl_list(raw_acl, item_id=item.id)

                if not acl_list and not acl_failed and self.acl_owner:
                    acl_list = [self.acl_owner]

            # Unified dedup checks
            latest = self.metadata_tracker.get_latest_record(item_name)
            stored_acl = (latest.metadata_content or {}).get("acl", []) if latest else []
            acl_changed = self.acl_enabled and sorted(stored_acl) != acl_list
            if latest and latest.checksum == new_checksum and not acl_changed:
                logger.info(f"Skipping unchanged item: {item_name}")
                return 0
            seen_key = f"{item.id}:{new_checksum}"
            if not self._seen_add(seen_key):
                logger.info(f"Skipping duplicate checksum for item: {item.id}")
                return 0

            # Fetch content for the fast path only after dedup checks pass —
            # avoids the expensive API call when the item is unchanged or already seen.
            if raw_content is None:
                raw_content = self.get_raw_content(item)
                if not raw_content.strip():
                    logger.warning(f"Skipping empty content for item: {item.id}")
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
            reserved_keys = set(BaseMetadataSchema.model_fields) | {"acl"}
            filtered_extra = {k: v for k, v in extra.items() if k not in reserved_keys}
            metadata.update(filtered_extra)

            if self.acl_enabled:
                metadata["acl"] = acl_list

            excluded_keys = ["acl"] if self.acl_enabled else []
            doc = Document(
                text=raw_content,
                metadata=metadata,
                excluded_llm_metadata_keys=excluded_keys,
                excluded_embed_metadata_keys=excluded_keys,
            )

            self.vector_manager.insert_documents([doc])

            extra_metadata = {"source_name": self.source_name}
            if self.acl_enabled:
                extra_metadata["acl"] = acl_list

            self.metadata_tracker.record_metadata(
                item_name,
                new_checksum,
                version,
                1,
                last_modified_ts,
                extra_metadata=extra_metadata,
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
