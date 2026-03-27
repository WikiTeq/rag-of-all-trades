# Standard library imports
import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

# Third-party imports
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem

logger = logging.getLogger(__name__)


class SlackIngestionJob(IngestionJob):
    """Ingestion connector for Slack workspaces.

    Uses the Slack SDK WebClient directly for channel discovery and message
    fetching. Each message (with its thread replies) becomes an individual
    IngestionItem, compatible with the base class contract.

    Configuration (config.yaml):
        - config.token: Slack bot token (required)
        - config.channel_ids: Comma-separated channel IDs (mutually exclusive with channel_patterns)
        - config.channel_patterns: Comma-separated channel name patterns / regex (mutually exclusive with channel_ids)
        - config.channel_types: Comma-separated channel types for pattern discovery,
            default "public_channel,private_channel" (optional, only with channel_patterns)
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

        self._client = WebClient(token=self.token)

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
        """Resolve channel IDs and yield one IngestionItem per message.

        Each top-level message (with its thread replies concatenated) becomes
        a separate IngestionItem so the base class can checksum, dedup, chunk,
        and embed them individually.
        """
        logger.info(f"[{self.source_name}] Discovering Slack channels")

        channel_ids = self._resolve_channel_ids()

        logger.info(
            f"[{self.source_name}] Total channels to ingest: {len(channel_ids)}"
        )

        for channel_id in channel_ids:
            yield from self._yield_messages(channel_id)

    def get_raw_content(self, item: IngestionItem) -> str:
        """Return the text of a single Slack message (with thread replies)."""
        return item.source_ref.get("text", "") or ""

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe, unique identifier for the message."""
        channel_id = item.source_ref.get("channel_id", "")
        message_ts = item.source_ref.get("message_ts", "")
        raw = f"{channel_id}_{message_ts}"
        safe = re.sub(r"[^\w\-]", "_", raw)
        return safe[:255]

    def get_document_metadata(
        self,
        item: IngestionItem,
        item_name: str,
        checksum: str,
        version: int,
        last_modified: Any,
    ) -> Dict[str, Any]:
        """Build metadata dict with Slack-specific fields."""
        channel_id = item.source_ref.get("channel_id", "")
        message_ts = item.source_ref.get("message_ts", "")
        metadata = super().get_document_metadata(
            item, item_name, checksum, version, last_modified
        )
        # Convert ts "1234567890.123456" → "12345678901234​56" for Slack URL anchor
        ts_anchor = message_ts.replace(".", "")
        metadata.update(
            {
                "channel_id": channel_id,
                "message_ts": message_ts,
                "url": f"https://slack.com/app_redirect?channel={channel_id}&message_ts={ts_anchor}",
            }
        )
        return metadata

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _yield_messages(self, channel_id: str) -> Iterator[IngestionItem]:
        """Fetch all top-level messages from a channel and yield one IngestionItem each.

        Thread replies are fetched for each message and concatenated into its text.
        """
        client = self._client
        next_cursor = None
        earliest_ts = (
            str(self.earliest_date.timestamp()) if self.earliest_date else None
        )
        latest_ts = (
            str(self.latest_date.timestamp())
            if self.latest_date
            else str(datetime.now().timestamp())
        )

        while True:
            try:
                kwargs: Dict[str, Any] = {
                    "channel": channel_id,
                    "cursor": next_cursor,
                    "latest": latest_ts,
                }
                if earliest_ts:
                    kwargs["oldest"] = earliest_ts

                result = client.conversations_history(**kwargs)
                messages = result["messages"]

                logger.info(
                    f"[{self.source_name}] {len(messages)} message(s) fetched from {channel_id}"
                )

                for message in messages:
                    ts = message.get("ts", "")
                    text = self._fetch_message_with_replies(channel_id, ts)
                    last_modified = (
                        datetime.fromtimestamp(float(ts)) if ts else None
                    )
                    yield IngestionItem(
                        id=f"slack:{self.source_name}:{channel_id}:{ts}",
                        source_ref={
                            "channel_id": channel_id,
                            "message_ts": ts,
                            "text": text,
                        },
                        last_modified=last_modified,
                    )

                if not result["has_more"]:
                    break
                next_cursor = result["response_metadata"]["next_cursor"]

            except SlackApiError as e:
                error = e.response["error"]
                if error == "ratelimited":
                    retry_after = int(e.response.headers.get("retry-after", 1))
                    logger.error(
                        f"[{self.source_name}] Rate limited, sleeping {retry_after}s"
                    )
                    time.sleep(retry_after)
                elif error == "not_in_channel":
                    logger.error(
                        f"[{self.source_name}] Bot not in channel {channel_id}, skipping"
                    )
                    break
                else:
                    logger.error(
                        f"[{self.source_name}] Error fetching messages from {channel_id}: {e}"
                    )
                    break

    def _fetch_message_with_replies(self, channel_id: str, message_ts: str) -> str:
        """Fetch a message and its thread replies, returning all text concatenated."""
        client = self._client
        texts: List[str] = []
        next_cursor = None
        earliest_ts = (
            str(self.earliest_date.timestamp()) if self.earliest_date else None
        )
        latest_ts = (
            str(self.latest_date.timestamp())
            if self.latest_date
            else str(datetime.now().timestamp())
        )

        while True:
            try:
                kwargs: Dict[str, Any] = {
                    "channel": channel_id,
                    "ts": message_ts,
                    "cursor": next_cursor,
                    "latest": latest_ts,
                }
                if earliest_ts:
                    kwargs["oldest"] = earliest_ts

                result = client.conversations_replies(**kwargs)
                texts.extend(m["text"] for m in result["messages"])

                if not result["has_more"]:
                    break
                next_cursor = result["response_metadata"]["next_cursor"]

            except SlackApiError as e:
                error = e.response["error"]
                if error == "ratelimited":
                    retry_after = int(e.response.headers.get("retry-after", 1))
                    logger.error(
                        f"[{self.source_name}] Rate limited, sleeping {retry_after}s"
                    )
                    time.sleep(retry_after)
                else:
                    logger.error(
                        f"[{self.source_name}] Error fetching replies for {message_ts}: {e}"
                    )
                    break

        return "\n\n".join(texts)

    def _resolve_channel_ids(self) -> List[str]:
        """Return the list of channel IDs to ingest."""
        if self.channel_ids:
            return self.channel_ids

        if self.channel_patterns:
            try:
                ids = self._get_channel_ids_by_patterns(self.channel_patterns)
                logger.info(
                    f"[{self.source_name}] Resolved {len(ids)} channel(s) "
                    f"from patterns {self.channel_patterns}"
                )
                return ids
            except Exception as e:  # noqa: BLE001
                logger.error(
                    f"[{self.source_name}] Failed to resolve channel patterns: {e}"
                )
                return []

        logger.warning(
            f"[{self.source_name}] No channel_ids or channel_patterns configured, "
            "nothing to ingest"
        )
        return []

    def _get_channel_ids_by_patterns(self, patterns: List[str]) -> List[str]:
        """List all accessible channels and return IDs matching the given patterns."""
        result = self._client.conversations_list(types=self.channel_types)
        channels = result.get("channels", [])

        matched: List[str] = []
        exact_names = [p for p in patterns if not re.search(r"[\\^$.*+?()[\]{}|]", p)]
        regex_patterns = [p for p in patterns if re.search(r"[\\^$.*+?()[\]{}|]", p)]

        for channel in channels:
            name = channel.get("name", "")
            if name in exact_names:
                matched.append(channel["id"])
            elif any(re.match(pat, name) for pat in regex_patterns):
                matched.append(channel["id"])

        return list(set(matched))

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
