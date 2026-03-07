# Standard library imports
import logging
import re
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

# Third-party imports
from llama_index.readers.slack import SlackReader

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)


class SlackIngestionJob(IngestionJob):
    """Ingestion connector for Slack workspaces.

    Uses the LlamaIndex SlackReader for all channel discovery, content
    fetching, thread replies, and rate-limit handling. This connector only
    adds ROAT-specific orchestration: config parsing, validation, and
    IngestionItem production.

    Configuration (config.yaml):
        - config.token: Slack bot token (required)
        - config.channel_ids: Comma-separated channel IDs (mutually exclusive with channel_patterns)
        - config.channel_patterns: Comma-separated channel name patterns / regex (mutually exclusive with channel_ids)
        - config.channel_types: Comma-separated channel types for pattern discovery,
            default "public_channel,private_channel" (optional, only used with channel_patterns)
        - config.earliest_date: Earliest date to fetch messages from, e.g. "2024-01-01" (optional)
        - config.latest_date: Latest date to fetch messages up to, e.g. "2025-01-01" (optional)
        - config.schedules: Celery schedule in seconds (optional)

    Constraints:
        - channel_ids and channel_patterns are mutually exclusive
        - latest_date requires earliest_date
        - channel_types is only meaningful with channel_patterns
    """

    @property
    def source_type(self) -> str:
        return "slack"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        self.token = cfg.get("token", "").strip()
        if not self.token:
            raise ValueError("token is required in Slack connector config")

        self.channel_ids: List[str] = self._parse_ids(cfg.get("channel_ids", ""))
        self.channel_patterns: List[str] = self._parse_ids(
            cfg.get("channel_patterns", "")
        )

        if self.channel_ids and self.channel_patterns:
            raise ValueError(
                "channel_ids and channel_patterns are mutually exclusive in Slack connector config"
            )

        self.channel_types: str = cfg.get(
            "channel_types", "public_channel,private_channel"
        ).strip()

        earliest_date_str = cfg.get("earliest_date", "").strip()
        latest_date_str = cfg.get("latest_date", "").strip()

        self.earliest_date: Optional[datetime] = (
            self._parse_date(earliest_date_str) if earliest_date_str else None
        )
        self.latest_date: Optional[datetime] = (
            self._parse_date(latest_date_str) if latest_date_str else None
        )

        if self.latest_date and not self.earliest_date:
            raise ValueError(
                "earliest_date is required when latest_date is set in Slack connector config"
            )

        self._reader = SlackReader(
            slack_token=self.token,
            earliest_date=self.earliest_date,
            latest_date=self.latest_date,
            channel_types=self.channel_types,
        )

        logger.info(
            f"Initialized Slack connector "
            f"(channel_ids={self.channel_ids}, "
            f"channel_patterns={self.channel_patterns}, "
            f"channel_types={self.channel_types!r}, "
            f"earliest_date={earliest_date_str or 'none'}, "
            f"latest_date={latest_date_str or 'none'})"
        )

    # ------------------------------------------------------------------
    # IngestionJob abstract method implementations
    # ------------------------------------------------------------------

    def list_items(self) -> Iterator[IngestionItem]:
        """Resolve channel IDs and yield one IngestionItem per channel.

        If channel_ids are configured, yields them directly.
        If channel_patterns are configured, uses SlackReader.get_channel_ids()
        to resolve patterns/regex against the workspace channel list.
        """
        logger.info(f"[{self.source_name}] Discovering Slack channels")

        channel_ids = self._resolve_channel_ids()

        logger.info(
            f"[{self.source_name}] Total channels to ingest: {len(channel_ids)}"
        )

        for channel_id in channel_ids:
            yield IngestionItem(
                id=f"slack:{channel_id}",
                source_ref=channel_id,
                last_modified=None,
            )

    def get_raw_content(self, item: IngestionItem) -> str:
        """Fetch and return the full text content of a Slack channel using
        the LlamaIndex SlackReader.

        The reader handles pagination, thread replies, and rate limiting.
        """
        channel_id: str = item.source_ref
        try:
            docs = self._reader.load_data(channel_ids=[channel_id])
        except Exception as e:
            logger.error(
                f"[{self.source_name}] Failed to read channel {channel_id}: {e}"
            )
            return ""
        if not docs:
            return ""
        return docs[0].text or ""

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe, unique identifier for the channel."""
        channel_id: str = item.source_ref
        safe_id = re.sub(r"[^\w\-]", "_", channel_id)
        return safe_id[:255]

    def get_document_metadata(
        self,
        item: IngestionItem,
        item_name: str,
        checksum: str,
        version: int,
        last_modified: Any,
    ) -> Dict[str, Any]:
        """Build metadata dict with Slack-specific fields."""
        channel_id: str = item.source_ref
        metadata = super().get_document_metadata(
            item, item_name, checksum, version, last_modified
        )
        metadata.update(
            {
                "channel_id": channel_id,
                "url": f"https://slack.com/app_redirect?channel={channel_id}",
            }
        )
        return metadata

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_channel_ids(self) -> List[str]:
        """Return the list of channel IDs to ingest.

        Uses explicit channel_ids if configured, otherwise resolves
        channel_patterns via the reader.
        """
        if self.channel_ids:
            return self.channel_ids

        if self.channel_patterns:
            try:
                ids = self._reader.get_channel_ids(
                    channel_patterns=self.channel_patterns
                )
                logger.info(
                    f"[{self.source_name}] Resolved {len(ids)} channel(s) "
                    f"from patterns {self.channel_patterns}"
                )
                return ids
            except Exception as e:
                logger.error(
                    f"[{self.source_name}] Failed to resolve channel patterns: {e}"
                )
                return []

        # Neither channel_ids nor channel_patterns — log and return empty
        logger.warning(
            f"[{self.source_name}] No channel_ids or channel_patterns configured, "
            "nothing to ingest"
        )
        return []

    @staticmethod
    def _parse_ids(value: Any) -> List[str]:
        """Parse a comma-separated string or list into a list of stripped strings."""
        if not value:
            return []
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        return [v.strip() for v in str(value).split(",") if v.strip()]

    @staticmethod
    def _parse_date(value: str) -> datetime:
        """Parse a date string (YYYY-MM-DD) into a datetime object."""
        try:
            return datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                f"Invalid date format {value!r} in Slack connector config. "
                "Expected YYYY-MM-DD."
            )
