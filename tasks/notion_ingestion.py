# Standard library imports
import logging
import re
import time
from typing import Any, Dict, Iterator, List

# Third-party imports
from llama_index.readers.notion import NotionPageReader

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)


class NotionIngestionJob(IngestionJob):
    """Ingestion connector for Notion workspaces.

    Uses the LlamaIndex NotionPageReader for all discovery, content fetching,
    block recursion, and retry/rate-limit handling. This connector only adds
    ROAT-specific orchestration: config parsing, IngestionItem production,
    and optional per-page request delay.

    Configuration (config.yaml):
        - config.integration_token: Notion integration token (required)
        - config.page_ids: Comma-separated list of page IDs to ingest (optional)
        - config.database_ids: Comma-separated list of database IDs to ingest (optional)
        - config.request_delay: Seconds to sleep between page reads, default 0 (optional)
        - config.schedules: Celery schedule in seconds (optional)

    If neither page_ids nor database_ids are provided, all accessible pages and
    databases in the workspace are ingested (load-all mode).
    """

    @property
    def source_type(self) -> str:
        return "notion"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        self.integration_token = cfg.get("integration_token", "").strip()
        if not self.integration_token:
            raise ValueError(
                "integration_token is required in Notion connector config"
            )

        self.page_ids: List[str] = self._parse_ids(cfg.get("page_ids", ""))
        self.database_ids: List[str] = self._parse_ids(
            cfg.get("database_ids", "")
        )

        self.request_delay = float(cfg.get("request_delay", 0))
        if self.request_delay < 0:
            raise ValueError("request_delay must be non-negative")

        self._reader = NotionPageReader(
            integration_token=self.integration_token
        )

        load_mode = (
            "all"
            if not self.page_ids and not self.database_ids
            else "selective"
        )
        logger.info(
            f"Initialized Notion connector "
            f"(mode={load_mode}, page_ids={self.page_ids}, "
            f"database_ids={self.database_ids}, request_delay={self.request_delay})"
        )

    # ------------------------------------------------------------------
    # IngestionJob abstract method implementations
    # ------------------------------------------------------------------

    def list_items(self) -> Iterator[IngestionItem]:
        """Discover all Notion page IDs via the LlamaIndex reader and yield
        one IngestionItem per page.

        In load-all mode (no page_ids/database_ids configured), calls
        reader.list_pages() and reader.list_databases() to discover everything
        accessible to the integration. Database pages are resolved via
        reader.query_database().
        """
        logger.info(f"[{self.source_name}] Discovering Notion pages")

        all_page_ids: List[str] = list(self.page_ids)

        if self.database_ids:
            for db_id in self.database_ids:
                try:
                    db_page_ids = self._reader.query_database(db_id)
                    logger.info(
                        f"[{self.source_name}] Database {db_id}: found {len(db_page_ids)} page(s)"
                    )
                    all_page_ids.extend(db_page_ids)
                except Exception as e:
                    logger.error(
                        f"[{self.source_name}] Failed to query database {db_id}: {e}"
                    )

        # Load-all mode: no explicit IDs configured
        if not self.page_ids and not self.database_ids:
            try:
                workspace_page_ids = self._reader.list_pages()
                all_page_ids.extend(workspace_page_ids)
                logger.info(
                    f"[{self.source_name}] Workspace: found {len(workspace_page_ids)} page(s)"
                )
            except Exception as e:
                logger.error(
                    f"[{self.source_name}] Failed to list workspace pages: {e}"
                )

            try:
                db_ids = self._reader.list_databases()
                logger.info(
                    f"[{self.source_name}] Workspace: found {len(db_ids)} database(s)"
                )
                for db_id in db_ids:
                    try:
                        db_page_ids = self._reader.query_database(db_id)
                        all_page_ids.extend(db_page_ids)
                    except Exception as e:
                        logger.error(
                            f"[{self.source_name}] Failed to query database {db_id}: {e}"
                        )
            except Exception as e:
                logger.error(
                    f"[{self.source_name}] Failed to list workspace databases: {e}"
                )

        # Deduplicate while preserving order
        seen: set = set()
        unique_page_ids = [
            pid for pid in all_page_ids if not (pid in seen or seen.add(pid))
        ]

        logger.info(
            f"[{self.source_name}] Total unique pages to ingest: {len(unique_page_ids)}"
        )

        for page_id in unique_page_ids:
            yield IngestionItem(
                id=f"notion:{page_id}",
                source_ref=page_id,
                last_modified=None,
            )

    def get_raw_content(self, item: IngestionItem) -> str:
        """Fetch and return the full text content of a Notion page using
        the LlamaIndex reader.

        The reader handles block recursion, retries, and rate limiting.
        An optional request_delay is applied after each page read.
        """
        page_id: str = item.source_ref
        try:
            text = self._reader.read_page(page_id)
        except Exception as e:
            logger.error(
                f"[{self.source_name}] Failed to read page {page_id}: {e}"
            )
            return ""
        finally:
            if self.request_delay > 0:
                time.sleep(self.request_delay)
        return text or ""

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe, unique identifier for the page."""
        page_id: str = item.source_ref
        safe_id = re.sub(r"[^\w\-]", "_", page_id)
        return safe_id[:255]

    def get_document_metadata(
        self,
        item: IngestionItem,
        item_name: str,
        checksum: str,
        version: int,
        last_modified: Any,
    ) -> Dict[str, Any]:
        """Build metadata dict with Notion-specific fields."""
        page_id: str = item.source_ref
        metadata = super().get_document_metadata(
            item, item_name, checksum, version, last_modified
        )
        metadata.update(
            {
                "id": page_id,
                "url": f"https://notion.so/{page_id.replace('-', '')}",
            }
        )
        return metadata

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_ids(value: Any) -> List[str]:
        """Parse a comma-separated string or list of IDs into a list of stripped strings."""
        if not value:
            return []
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        return [v.strip() for v in str(value).split(",") if v.strip()]
