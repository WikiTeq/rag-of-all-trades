# Standard library imports
import logging
import re
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

# Third-party imports
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Local imports
from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.parse import parse_list
from utils.text import slugify

logger = logging.getLogger(__name__)


class SlackIngestionJob(IngestionJob):
    """Ingestion connector for Slack workspaces.

    Uses the Slack SDK WebClient directly for channel discovery and message
    fetching. Each message and each thread reply becomes an individual
    IngestionItem, compatible with the base class contract.

    Configuration (config.yaml):
        - config.token: Slack bot token (required)
        - config.channel_ids: Comma-separated channel IDs (mutually exclusive with channel_patterns)
        - config.channel_patterns: Comma-separated channel name patterns / regex (mutually exclusive with channel_ids)
        - config.channel_types: Comma-separated channel types for pattern discovery,
            default "public_channel,private_channel" (optional, only with channel_patterns)
        - config.earliest_date: Earliest date to fetch messages from, e.g. "2024-01-01" (optional)
        - config.latest_date: Latest date to fetch messages up to, e.g. "2025-01-01" (optional)

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

        self.channel_ids: list[str] = parse_list(cfg.get("channel_ids", ""))
        self.channel_patterns: list[str] = parse_list(cfg.get("channel_patterns", ""))

        if self.channel_ids and self.channel_patterns:
            raise ValueError("channel_ids and channel_patterns are mutually exclusive in Slack connector config")

        self.channel_types: str = cfg.get("channel_types", "public_channel,private_channel").strip()

        earliest_date_str = cfg.get("earliest_date", "").strip()
        latest_date_str = cfg.get("latest_date", "").strip()

        self.earliest_date: datetime | None = self._parse_date(earliest_date_str) if earliest_date_str else None
        self.latest_date: datetime | None = self._parse_date(latest_date_str) if latest_date_str else None

        if self.latest_date and not self.earliest_date:
            raise ValueError("earliest_date is required when latest_date is set in Slack connector config")
        if self.earliest_date and self.latest_date and self.latest_date < self.earliest_date:
            raise ValueError("latest_date must be greater than or equal to earliest_date in Slack connector config")

        self._client = WebClient(token=self.token)
        self._user_cache: dict[str, str] = {}
        self._channel_name_cache: dict[str, str] = {}

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

        Each message (including individual thread replies) becomes a separate
        IngestionItem so the base class can checksum, dedup, chunk, and embed
        them individually.
        """
        logger.info(f"[{self.source_name}] Discovering Slack channels")

        channel_ids = self._resolve_channel_ids()

        logger.info(f"[{self.source_name}] Total channels to ingest: {len(channel_ids)}")

        for channel_id in channel_ids:
            yield from self._yield_messages(channel_id)

    def get_raw_content(self, item: IngestionItem) -> str:
        """Return the text of a single Slack message or thread reply."""
        return item.source_ref.get("text", "") or ""

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a filesystem-safe, unique identifier for the message."""
        channel_id = item.source_ref.get("channel_id", "")
        message_ts = item.source_ref.get("message_ts", "")
        return slugify(f"{self.source_name}_{channel_id}_{message_ts}")

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        """Return Slack-specific metadata fields."""
        channel_id = item.source_ref.get("channel_id", "")
        message_ts = item.source_ref.get("message_ts", "")
        thread_ts = item.source_ref.get("thread_ts")
        user_id = item.source_ref.get("user_id", "")
        return {
            "channel_id": channel_id,
            "channel_name": self._resolve_channel_name(channel_id),
            "message_ts": message_ts,
            "thread_ts": thread_ts,
            "username": self._resolve_username(user_id) if user_id else "",
            "url": self._get_permalink(channel_id, message_ts),
        }

    def _resolve_username(self, user_id: str) -> str:
        if user_id in self._user_cache:
            return self._user_cache[user_id]
        try:
            result = self._client.users_info(user=user_id)
            username = result["user"].get("real_name") or result["user"].get("name", user_id)
        except SlackApiError as e:
            logger.warning(f"[{self.source_name}] users_info failed for {user_id}: {e}")
            username = user_id
        self._user_cache[user_id] = username
        return username

    def _resolve_channel_name(self, channel_id: str) -> str:
        if channel_id in self._channel_name_cache:
            return self._channel_name_cache[channel_id]
        try:
            result = self._client.conversations_info(channel=channel_id)
            name = result["channel"].get("name", channel_id)
        except SlackApiError as e:
            logger.warning(f"[{self.source_name}] conversations_info failed for {channel_id}: {e}")
            name = channel_id
        self._channel_name_cache[channel_id] = name
        return name

    def _resolve_mentions(self, text: str) -> str:
        """Replace Slack mention tokens with human-readable names."""

        def replace_user(m: re.Match) -> str:
            return f"@{self._resolve_username(m.group(1))}"

        def replace_channel(m: re.Match) -> str:
            label = m.group(2)
            return f"#{label}" if label else f"#{self._resolve_channel_name(m.group(1))}"

        text = re.sub(r"<@([A-Z0-9]+)>", replace_user, text)
        text = re.sub(r"<#([A-Z0-9]+)(?:\|([^>]*))?> ?", replace_channel, text)
        return text

    def _get_permalink(self, channel_id: str, message_ts: str) -> str:
        try:
            result = self._client.chat_getPermalink(channel=channel_id, message_ts=message_ts)
            return result["permalink"]
        except SlackApiError as e:
            logger.warning(f"[{self.source_name}] chat.getPermalink failed for {channel_id}/{message_ts}: {e}")
            ts_anchor = message_ts.replace(".", "")
            return f"https://slack.com/app_redirect?channel={channel_id}&message_ts={ts_anchor}"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _yield_messages(self, channel_id: str) -> Iterator[IngestionItem]:
        """Fetch all top-level messages from a channel and yield one IngestionItem each.

        For threaded messages, replies are fetched and yielded as individual items.
        """
        client = self._client
        next_cursor = None
        earliest_ts = str(self.earliest_date.timestamp()) if self.earliest_date else None
        latest_ts = (
            str((self.latest_date + timedelta(days=1) - timedelta(microseconds=1)).timestamp())
            if self.latest_date
            else str(datetime.now(UTC).timestamp())
        )

        while True:
            try:
                kwargs: dict[str, Any] = {
                    "channel": channel_id,
                    "cursor": next_cursor,
                    "latest": latest_ts,
                }
                if earliest_ts:
                    kwargs["oldest"] = earliest_ts

                result = client.conversations_history(**kwargs)
                messages = result["messages"]

                logger.info(f"[{self.source_name}] {len(messages)} message(s) fetched from {channel_id}")

                for message in messages:
                    # Skip all system/bot messages (join, leave, file, thread_broadcast, etc.)
                    if message.get("subtype"):
                        continue
                    # Skip thread replies appearing in channel history
                    ts = message.get("ts", "")
                    thread_ts = message.get("thread_ts")
                    if thread_ts and thread_ts != ts:
                        continue
                    has_replies = int(message.get("reply_count", 0)) > 0
                    if has_replies:
                        yield from self._yield_thread_messages(channel_id, ts)
                    else:
                        text = self._resolve_mentions(message.get("text", "") or "")
                        last_modified = datetime.fromtimestamp(float(ts), tz=UTC) if ts else None
                        yield IngestionItem(
                            id=f"slack:{self.source_name}:{channel_id}:{ts}",
                            source_ref={
                                "channel_id": channel_id,
                                "message_ts": ts,
                                "thread_ts": None,
                                "user_id": message.get("user", ""),
                                "text": text,
                            },
                            last_modified=last_modified,
                        )

                # TODO: track latest ingested TS per channel and use it as oldest on subsequent
                # runs to skip already-seen messages and reduce API calls (at the cost of missing edits)
                if not result["has_more"]:
                    break
                next_cursor = result["response_metadata"]["next_cursor"]

            except SlackApiError as e:
                error = e.response["error"]
                if error == "ratelimited":
                    retry_after = int(e.response.headers.get("retry-after", 1))
                    logger.error(f"[{self.source_name}] Rate limited, sleeping {retry_after}s")
                    time.sleep(retry_after)
                elif error == "not_in_channel":
                    logger.error(f"[{self.source_name}] Bot not in channel {channel_id}, skipping")
                    break
                else:
                    logger.error(f"[{self.source_name}] Error fetching messages from {channel_id}: {e}")
                    break

    def _yield_thread_messages(self, channel_id: str, thread_ts: str) -> Iterator[IngestionItem]:
        """Fetch all messages in a thread and yield each as an individual IngestionItem."""
        client = self._client
        next_cursor = None
        earliest_ts = str(self.earliest_date.timestamp()) if self.earliest_date else None
        latest_ts = (
            str((self.latest_date + timedelta(days=1) - timedelta(microseconds=1)).timestamp())
            if self.latest_date
            else str(datetime.now(UTC).timestamp())
        )

        while True:
            try:
                kwargs: dict[str, Any] = {
                    "channel": channel_id,
                    "ts": thread_ts,
                    "cursor": next_cursor,
                    "latest": latest_ts,
                }
                if earliest_ts:
                    kwargs["oldest"] = earliest_ts

                result = client.conversations_replies(**kwargs)
                for m in result.get("messages", []):
                    if m.get("subtype"):
                        continue
                    ts = m.get("ts", "")
                    text = m.get("text")
                    if not text:
                        continue
                    last_modified = datetime.fromtimestamp(float(ts), tz=UTC) if ts else None
                    yield IngestionItem(
                        id=f"slack:{self.source_name}:{channel_id}:{ts}",
                        source_ref={
                            "channel_id": channel_id,
                            "message_ts": ts,
                            "thread_ts": thread_ts,
                            "user_id": m.get("user", ""),
                            "text": self._resolve_mentions(text),
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
                    logger.error(f"[{self.source_name}] Rate limited, sleeping {retry_after}s")
                    time.sleep(retry_after)
                else:
                    logger.error(f"[{self.source_name}] Error fetching replies for {thread_ts}: {e}")
                    break

    def _resolve_channel_ids(self) -> list[str]:
        """Return the list of channel IDs to ingest."""
        if self.channel_ids:
            return self.channel_ids

        if self.channel_patterns:
            try:
                ids = self._get_channel_ids_by_patterns(self.channel_patterns)
                logger.info(
                    f"[{self.source_name}] Resolved {len(ids)} channel(s) from patterns {self.channel_patterns}"
                )
                return ids
            except Exception as e:  # noqa: BLE001
                logger.error(f"[{self.source_name}] Failed to resolve channel patterns: {e}")
                return []

        logger.warning(f"[{self.source_name}] No channel_ids or channel_patterns configured, nothing to ingest")
        return []

    def _get_channel_ids_by_patterns(self, patterns: list[str]) -> list[str]:
        """List all accessible channels and return IDs matching the given patterns."""
        exact_names = [p for p in patterns if not re.search(r"[\\^$.*+?()[\]{}|]", p)]
        regex_patterns = [p for p in patterns if re.search(r"[\\^$.*+?()[\]{}|]", p)]

        seen: dict[str, None] = {}
        cursor = None

        while True:
            kwargs: dict[str, Any] = {"types": self.channel_types}
            if cursor:
                kwargs["cursor"] = cursor
            result = self._client.conversations_list(**kwargs)
            channels = result.get("channels", [])

            for channel in channels:
                name = channel.get("name", "")
                channel_id = channel["id"]
                if name in exact_names or any(re.match(pat, name) for pat in regex_patterns):
                    seen[channel_id] = None

            cursor = result.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break

        return list(seen)

    @staticmethod
    def _parse_date(value: str) -> datetime:
        """Parse a date string (YYYY-MM-DD) into a datetime object."""
        try:
            return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
        except ValueError:
            raise ValueError(f"Invalid date format {value!r} in Slack connector config. Expected YYYY-MM-DD.")
