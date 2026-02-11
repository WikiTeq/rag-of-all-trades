from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Iterable
import gc
import hashlib
import logging
import time
from typing import Dict, Any
from llama_index.core import Document
from tasks.helper_classes.metadata_tracker import MetadataTracker
from tasks.helper_classes.vector_store import VectorStoreManager
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)

# Keys that process_item sets; get_extra_metadata must not overwrite these.
RESERVED_METADATA_KEYS = frozenset({
    "source", "key", "checksum", "version", "format",
    "source_name", "file_name", "last_modified",
})


class IngestionJob(ABC):
    """Abstract base class for all ingestion jobs that process content from various sources.

    This class provides the core framework for ingesting content from different data sources
    (files, APIs, databases, etc.) into a vector store for RAG applications. It handles
    duplicate detection, versioning, metadata tracking, and provides hooks for customization.
    """

    def __init__(self, config: dict):
        """Initialize the ingestion job with configuration and core components.

        Args:
            config: Dictionary containing job configuration including source name and settings

        Sets up metadata tracking, vector store management, and duplicate detection infrastructure.
        """
        self.config = config
        self.source_name = config.get("name")
        self.metadata_tracker = MetadataTracker()
        self.vector_manager = VectorStoreManager()

        # Rate limiting
        cfg = config.get("config", {})
        self.request_delay = float(cfg.get("request_delay", 0.0))

        # Seen checksums - prevent reprocessing identical content
        self._seen_capacity = 10000
        self._seen = OrderedDict()

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

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Hook for subclasses to provide additional metadata.

        Default implementation returns an empty dictionary. Subclasses can override
        this to add source-specific fields (e.g., URLs, tags, etc.) without
        needing to construct the standard metadata dictionary. Keys that match
        RESERVED_METADATA_KEYS (source, key, checksum, version, format,
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
            checksum: MD5 hash of the content

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
        1. Fetch raw content
        2. Check for duplicates and emptiness
        3. Generate checksum and check for changes
        4. Handle versioning and cleanup of old embeddings
        5. Create document with metadata
        6. Store in vector database and update metadata tracking

        Args:
            item: The ingestion item to process

        Returns:
            int: 1 if item was successfully ingested, 0 if skipped or failed
        """
        try:
            # Get raw content
            raw_content = self.get_raw_content(item)

            if not raw_content.strip():
                logger.debug(f"Skipping empty content for item: {item.id}")
                return 0

            new_checksum = hashlib.md5(raw_content.encode("utf-8")).hexdigest()

            # skip if duplicate
            if not self._seen_add(new_checksum):
                logger.debug(f"Skipping duplicate checksum for item: {item.id}")
                return 0

            item_name = self.get_item_name(item)

            # Extract last_modified from ingestion item
            last_modified = item.last_modified

            # existing metadata
            latest = self.metadata_tracker.get_latest_record(item_name)
            if latest and latest.checksum == new_checksum:
                logger.debug(f"Skipping unchanged item: {item_name}")
                return 0

            # delete previous embeddings if updated
            if latest:
                logger.info(f"Updating item {item_name} from version {latest.version}")
                self.metadata_tracker.delete_previous_embeddings(item_name)

            version = (latest.version + 1) if latest else 1

            # Standard metadata (reserved keys must not be overwritten by get_extra_metadata)
            metadata = {
                "source": self.source_type,
                "key": item_name,
                "checksum": new_checksum,
                "version": version,
                "format": "markdown",
                "source_name": self.source_name,
                "file_name": item_name,
                "last_modified": str(last_modified),
            }

            extra = self.get_extra_metadata(item, raw_content, metadata)
            for k, v in extra.items():
                if k not in RESERVED_METADATA_KEYS:
                    metadata[k] = v

            docs = Document(
                    text=raw_content,
                    metadata=metadata
                )

            self.vector_manager.insert_documents([docs])

            self.metadata_tracker.record_metadata(
                item_name,
                new_checksum,
                version,
                1,
                last_modified,
                extra_metadata={"source_name": self.source_name}
            )

            logger.info(f"Successfully ingested: {item_name} (version {version})")

            del raw_content
            gc.collect()
            return 1

        except Exception as e:
            logger.exception(f"Failed to process item {item}: {e}")
            return 0  # Return 0 to continue processing other items

    def run(self):
        """Execute the complete ingestion job for this data source.

        Discovers all items using list_items(), processes each one through process_item(),
        and provides comprehensive progress tracking and error reporting. Continues
        processing even if individual items fail.

        Returns:
            str: Summary message indicating total items processed, skipped, and any errors
        """
        total = 0
        skipped = 0

        logger.info(f"[{self.source_name}] Starting ingestion job")

        try:
            for item in self.list_items():
                if self.request_delay > 0:
                    time.sleep(self.request_delay)

                count = self.process_item(item)
                if count == 0:
                    skipped += 1
                else:
                    total += count

            result_msg = f"[{self.source_name}] Completed: {total} ingested, {skipped} skipped"
            logger.info(result_msg)
            return result_msg

        except Exception as e:
            error_msg = f"[{self.source_name}] Job failed: {e}"
            logger.exception(error_msg)
            return f"{error_msg}. Partial results: {total} ingested, {skipped} skipped"
