import hashlib
import logging
import re
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import requests

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.parse import parse_list, parse_timestamp
from utils.text import slugify

logger = logging.getLogger(__name__)

CHANNEL_TEXT = 0
CHANNEL_ANNOUNCEMENT = 5
CHANNEL_FORUM = 15
CHANNEL_MEDIA = 16
THREAD_TYPES = {10, 11, 12}
MESSAGE_CONTAINER_TYPES = {CHANNEL_TEXT, CHANNEL_ANNOUNCEMENT}
THREAD_CONTAINER_TYPES = {CHANNEL_TEXT, CHANNEL_ANNOUNCEMENT, CHANNEL_FORUM, CHANNEL_MEDIA}
PRIVATE_THREAD_PARENT_TYPES = {CHANNEL_TEXT}
INGESTED_MESSAGE_TYPES = {0, 19, 20, 23}  # DEFAULT, REPLY, CHAT_INPUT_COMMAND, CONTEXT_MENU_COMMAND
PAGE_SIZE = 100
EMPTY_MESSAGES_BEFORE_WARNING = 5
ACTIVE_THREADS_LABEL = "active threads"
PUBLIC_ARCHIVE_LABEL = "public archived threads"
REQUIRED_THREAD_LISTINGS = {ACTIVE_THREADS_LABEL, PUBLIC_ARCHIVE_LABEL}
SNOWFLAKE_RE = re.compile(r"^[0-9]{1,20}$")


class DiscordAPIError(Exception):
    """Raised when the Discord API returns an error or retries are exhausted."""

    def __init__(self, message: str, status: int | None = None, code: int | None = None):
        """Store the HTTP status and Discord error code next to the message."""
        super().__init__(message)
        self.status = status
        self.code = code


class DiscordAuthError(DiscordAPIError):
    """Raised on HTTP 401. Never retried."""


class DiscordClient:
    """Minimal read-only Discord REST client with Discord-aware rate-limit handling.

    - HTTP 429: waits for the JSON ``retry_after`` (fractional seconds), falling back to the
      ``Retry-After`` header, then to exponential backoff. Requests are sequential, so waiting out a
      global limit also covers every other route. The log line tells global and route limits apart.
    - Bucket exhaustion: when ``X-RateLimit-Remaining`` is 0, waits ``X-RateLimit-Reset-After`` before
      the next request.
    - Network errors and HTTP 5xx: exponential backoff.
    - Every retry loop is bounded by ``max_retries``; a wait longer than ``max_wait`` fails at once.
    - HTTP 401 raises ``DiscordAuthError`` without retrying.

    Rate-limit state lives in this process only. Concurrent workers that share one bot token
    coordinate only through 429 responses.
    """

    BASE_URL = "https://discord.com/api/v10"

    def __init__(
        self,
        token: str,
        max_retries: int = 5,
        timeout: float = 30,
        max_wait: float = 120,
    ) -> None:
        """Create the HTTP session with the bot token and retry limits."""
        self.max_retries = max_retries
        self.timeout = timeout
        self.max_wait = max_wait
        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bot {token}", "User-Agent": "rag-of-all-trades-discord"})

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """GET a Discord API path and return the decoded JSON, retrying per the class rules."""
        url = f"{self.BASE_URL}{path}"
        for attempt in range(self.max_retries + 1):
            try:
                resp = self._session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                if attempt >= self.max_retries:
                    raise DiscordAPIError(f"Discord request to {path} failed: {exc}") from exc
                self._sleep(self._backoff(attempt), f"network error on {path}: {exc}")
                continue

            status = resp.status_code
            if status == 429:
                if attempt >= self.max_retries:
                    raise DiscordAPIError(f"Discord rate limit on {path} persisted after retries", status=429)
                self._handle_rate_limit(resp, attempt)
                continue
            if status >= 500:
                if attempt >= self.max_retries:
                    raise DiscordAPIError(f"Discord server error {status} on {path} after retries", status=status)
                self._sleep(self._backoff(attempt), f"server error {status} on {path}")
                continue
            if status == 401:
                raise DiscordAuthError("Discord rejected the bot token (HTTP 401)", status=401)
            if status >= 400:
                raise DiscordAPIError(
                    f"Discord returned HTTP {status} for {path}: {self._error_message(resp)}",
                    status=status,
                    code=self._error_code(resp),
                )

            self._wait_for_bucket_reset(resp)
            return resp.json()
        raise DiscordAPIError(f"Discord request to {path} failed")

    @staticmethod
    def _backoff(attempt: int) -> float:
        """Return the exponential backoff delay in seconds for a retry attempt."""
        return float(2**attempt)

    @staticmethod
    def _json_body(resp: requests.Response) -> dict:
        """Return the response body as a dict, or an empty dict when it is not a JSON object."""
        try:
            body = resp.json()
        except ValueError:
            return {}
        return body if isinstance(body, dict) else {}

    @staticmethod
    def _to_float(value: Any) -> float | None:
        """Convert a value to float, or return None when it is not numeric."""
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _error_message(cls, resp: requests.Response) -> str:
        """Return the Discord error message of a response, or an empty string."""
        return str(cls._json_body(resp).get("message", ""))

    @classmethod
    def _error_code(cls, resp: requests.Response) -> int | None:
        """Return the numeric Discord error code of a response, or None."""
        code = cls._json_body(resp).get("code")
        return code if isinstance(code, int) else None

    def _handle_rate_limit(self, resp: requests.Response, attempt: int) -> None:
        """Wait out an HTTP 429, or raise when the requested wait exceeds ``max_wait``."""
        body = self._json_body(resp)
        wait = self._to_float(body.get("retry_after"))
        if wait is None:
            wait = self._to_float(resp.headers.get("Retry-After"))
        if wait is None:
            wait = self._backoff(attempt)
        wait = max(wait, 0.0)
        is_global = bool(body.get("global")) or resp.headers.get("X-RateLimit-Scope") == "global"
        if wait > self.max_wait:
            raise DiscordAPIError(
                f"Discord rate limit asks for a {wait:.0f}s wait, above the {self.max_wait:.0f}s limit", status=429
            )
        self._sleep(wait, "global rate limit" if is_global else "route rate limit")

    def _wait_for_bucket_reset(self, resp: requests.Response) -> None:
        """Wait for the bucket reset when the response shows that the bucket is exhausted."""
        if resp.headers.get("X-RateLimit-Remaining") != "0":
            return
        reset_after = self._to_float(resp.headers.get("X-RateLimit-Reset-After"))
        if reset_after is not None and 0 < reset_after <= self.max_wait:
            self._sleep(reset_after, "bucket exhausted")

    @staticmethod
    def _sleep(seconds: float, reason: str) -> None:
        """Log the reason and sleep for the given number of seconds."""
        logger.warning("Discord: waiting %.2fs (%s)", seconds, reason)
        time.sleep(seconds)


@dataclass
class _ChannelOutcome:
    """What one run did with a channel or thread, so that a second selection of it behaves consistently."""

    items: int = 0
    error: DiscordAPIError | None = None


class DiscordIngestionJob(IngestionJob):
    """Ingestion connector for Discord servers (guilds) using a bot token.

    Reads the Discord REST API directly. Each message, including each message inside a thread,
    becomes one IngestionItem. A selected channel includes its active and archived threads. Forum
    and media channels are read through their posts (threads).

    Configuration (config.yaml):
        - config.bot_token: Discord bot token (required)
        - config.guild_ids: Guild IDs, YAML list or comma-separated string (optional)
        - config.channel_ids: Channel or thread IDs, YAML list or comma-separated string (optional)

    At least one of guild_ids and channel_ids is required. With guild_ids only, every text,
    announcement, forum and media channel of those guilds is read. With channel_ids only, only those
    channels are read. With both, channel_ids must belong to the listed guilds.
    """

    @property
    def source_type(self) -> str:
        """Return the source type identifier."""
        return "discord"

    def __init__(self, config: dict):
        """Validate the Discord config and build the REST client."""
        super().__init__(config)

        cfg = config.get("config", {})

        self.bot_token = str(cfg.get("bot_token") or "").strip()
        if not self.bot_token:
            raise ValueError("bot_token is required in Discord connector config")

        self.guild_ids = self._parse_ids(cfg.get("guild_ids"), "guild_ids")
        self.channel_ids = self._parse_ids(cfg.get("channel_ids"), "channel_ids")
        if not self.guild_ids and not self.channel_ids:
            raise ValueError("At least one of guild_ids or channel_ids is required in Discord connector config")

        self._client = DiscordClient(self.bot_token)
        self._channel_names: dict[str, str] = {}
        self._reset_run_state()

    def _reset_run_state(self) -> None:
        """Clear per-run tracking so that each ``list_items()`` call starts clean."""
        self._outcomes: dict[str, _ChannelOutcome] = {}
        self._seen_threads: set[str] = set()
        self._active_threads: dict[str, list[dict]] = {}
        self._inaccessible = 0

    # ------------------------------------------------------------------
    # Abstract method implementations
    # ------------------------------------------------------------------

    def list_items(self) -> Iterator[IngestionItem]:
        """Resolve the selected channels and yield one IngestionItem per message."""
        self._reset_run_state()
        try:
            targets = self._resolve_targets()
            logger.info(f"[{self.source_name}] Discord channels to ingest: {len(targets)}")

            for channel, explicit in targets:
                yield from self._read_target(channel, explicit)

            if self._inaccessible:
                logger.warning(
                    f"[{self.source_name}] {self._inaccessible} Discord channel(s), thread(s) or thread listing(s) "
                    "were not accessible to the bot and were skipped"
                )
        finally:
            self._client.close()

    def _read_target(self, channel: dict, explicit: bool) -> Iterator[IngestionItem]:
        """Yield the messages of a channel or thread and of the threads inside a channel."""
        channel_type = channel.get("type")
        if channel_type in THREAD_TYPES:
            yield from self._yield_messages(channel, None, explicit=True)
            return
        if channel_type in MESSAGE_CONTAINER_TYPES:
            yield from self._yield_messages(channel, None, explicit=explicit)
        else:
            self._outcomes.setdefault(channel.get("id"), _ChannelOutcome())
        for thread in self._discover_threads(channel, explicit):
            yield from self._yield_messages(thread, channel, explicit=False)

    def get_raw_content(self, item: IngestionItem) -> str:
        """Return the rendered text of a single Discord message."""
        return item.source_ref.get("text", "") or ""

    def get_item_name(self, item: IngestionItem) -> str:
        """Return a unique tracker key built from the source name and Discord IDs."""
        source_name = self.source_name or ""
        digest = hashlib.md5(source_name.encode("utf-8"), usedforsecurity=False).hexdigest()[:8]
        ref = item.source_ref
        return (
            f"discord_{slugify(source_name, max_len=60)}_{digest}"
            f"_{ref['guild_id']}_{ref['thread_id'] or ref['channel_id']}_{ref['message_id']}"
        )

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        """Return Discord-specific metadata fields."""
        ref = item.source_ref
        return {
            "guild_id": ref["guild_id"],
            "channel_id": ref["channel_id"],
            "channel_name": ref["channel_name"],
            "thread_id": ref["thread_id"],
            "thread_name": ref["thread_name"],
            "message_id": ref["message_id"],
            "author_id": ref["author_id"],
            "author_name": ref["author_name"],
            "author_is_bot": ref["author_is_bot"],
            "created_at": ref["created_at"],
            "edited_at": ref["edited_at"],
            "reply_to_message_id": ref["reply_to_message_id"],
            "url": ref["url"],
        }

    # ------------------------------------------------------------------
    # Config parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_ids(value: Any, field: str) -> list[str]:
        """Parse a list of snowflake IDs, drop duplicates and reject invalid values."""
        ids = parse_list(value)
        for item in ids:
            if not SNOWFLAKE_RE.match(item):
                raise ValueError(f"Invalid Discord ID {item!r} in {field}. Expected a numeric snowflake.")
        return list(dict.fromkeys(ids))

    # ------------------------------------------------------------------
    # Target resolution
    # ------------------------------------------------------------------

    def _resolve_targets(self) -> list[tuple[dict, bool]]:
        """Return (channel, explicit) pairs. Explicit targets fail loudly when inaccessible."""
        targets: dict[str, tuple[dict, bool]] = {}
        if self.channel_ids:
            for channel_id in self.channel_ids:
                channel = self._client.get_json(f"/channels/{channel_id}")
                self._validate_channel(channel_id, channel)
                channel["id"] = channel_id
                self._remember_name(channel)
                if channel["type"] in THREAD_TYPES:
                    self._remember_parent_name(channel)
                targets.setdefault(channel_id, (channel, True))
            return list(targets.values())

        last_error: DiscordAPIError | None = None
        for guild_id in self.guild_ids:
            try:
                channels = self._client.get_json(f"/guilds/{guild_id}/channels")
            except DiscordAPIError as exc:
                if isinstance(exc, DiscordAuthError) or exc.status not in (403, 404):
                    raise
                last_error = exc
                self._inaccessible += 1
                logger.warning(f"[{self.source_name}] Cannot list channels of Discord guild {guild_id}: {exc}")
                continue
            for channel in channels:
                channel_id = channel.get("id")
                if not channel_id:
                    continue
                self._remember_name(channel)
                if channel.get("type") in THREAD_CONTAINER_TYPES:
                    channel.setdefault("guild_id", guild_id)
                    targets.setdefault(channel_id, (channel, False))
        if last_error is not None and self._inaccessible == len(self.guild_ids):
            raise last_error
        return list(targets.values())

    def _validate_channel(self, channel_id: str, channel: dict) -> None:
        """Raise ValueError when a selected channel has an unsupported type or the wrong guild."""
        channel_type = channel.get("type")
        if channel_type not in THREAD_CONTAINER_TYPES and channel_type not in THREAD_TYPES:
            raise ValueError(
                f"Discord channel {channel_id} has unsupported type {channel_type}. Supported: text, "
                "announcement, forum, media channels and threads."
            )
        guild_id = channel.get("guild_id")
        if not guild_id:
            raise ValueError(f"Discord channel {channel_id} does not belong to a guild")
        if self.guild_ids and guild_id not in self.guild_ids:
            raise ValueError(f"Discord channel {channel_id} belongs to guild {guild_id}, which is not in guild_ids")

    def _remember_parent_name(self, thread: dict) -> None:
        """Cache the parent channel name of a directly selected thread so its text matches a parent-driven read."""
        parent_id = thread.get("parent_id")
        if not parent_id or parent_id in self._channel_names:
            return
        try:
            self._remember_name(self._client.get_json(f"/channels/{parent_id}"))
        except DiscordAPIError as exc:
            if isinstance(exc, DiscordAuthError) or exc.status not in (403, 404):
                raise
            logger.warning(
                f"[{self.source_name}] Cannot read parent channel {parent_id} of thread {thread['id']}: {exc}"
            )

    def _remember_name(self, channel: dict) -> None:
        """Cache the name of a channel or thread for mention resolution and rendering."""
        if channel.get("id") and channel.get("name"):
            self._channel_names[channel["id"]] = channel["name"]

    # ------------------------------------------------------------------
    # Thread discovery
    # ------------------------------------------------------------------

    def _discover_threads(self, channel: dict, explicit: bool) -> Iterator[dict]:
        """Yield active and archived threads of a channel, each thread once.

        For an explicitly selected channel, a failure to list active or public archived threads
        raises. Failures on the private archive routes only warn, since they need extra permissions.
        """
        channel_id = channel.get("id")
        channel_type = channel.get("type")
        sources: list[tuple[str, Callable[[], Iterator[dict]]]] = [
            (ACTIVE_THREADS_LABEL, lambda: iter(self._get_active_threads(channel.get("guild_id"), channel_id))),
            (
                PUBLIC_ARCHIVE_LABEL,
                lambda: self._paginate_archived(
                    f"/channels/{channel_id}/threads/archived/public", self._archive_timestamp_cursor
                ),
            ),
        ]
        if channel_type in PRIVATE_THREAD_PARENT_TYPES:
            sources.append(
                (
                    "joined private archived threads",
                    lambda: self._paginate_archived(
                        f"/channels/{channel_id}/users/@me/threads/archived/private", lambda t: t.get("id")
                    ),
                )
            )
            sources.append(
                (
                    "private archived threads",
                    lambda: self._paginate_archived(
                        f"/channels/{channel_id}/threads/archived/private", self._archive_timestamp_cursor
                    ),
                )
            )

        for label, make_iterator in sources:
            try:
                for thread in make_iterator():
                    self._remember_name(thread)
                    if thread.get("id") and thread["id"] not in self._seen_threads:
                        self._seen_threads.add(thread["id"])
                        thread.setdefault("guild_id", channel.get("guild_id"))
                        yield thread
            except DiscordAPIError as exc:
                if isinstance(exc, DiscordAuthError) or exc.status not in (403, 404):
                    raise
                if explicit and label in REQUIRED_THREAD_LISTINGS:
                    raise
                self._inaccessible += 1
                logger.warning(f"[{self.source_name}] Cannot list {label} of channel {channel_id}: {exc}")

    def _get_active_threads(self, guild_id: str, parent_id: str) -> list[dict]:
        """Return the active threads of a parent channel, fetching each guild listing once."""
        if guild_id not in self._active_threads:
            data = self._client.get_json(f"/guilds/{guild_id}/threads/active")
            self._active_threads[guild_id] = data.get("threads") or []
        return [t for t in self._active_threads[guild_id] if t.get("parent_id") == parent_id]

    @staticmethod
    def _archive_timestamp_cursor(thread: dict) -> str | None:
        """Return the archive timestamp that Discord uses as pagination cursor."""
        return (thread.get("thread_metadata") or {}).get("archive_timestamp")

    def _paginate_archived(self, path: str, cursor_of: Callable[[dict], str | None]) -> Iterator[dict]:
        """Yield archived threads page by page and raise when the cursor does not advance."""
        before: str | None = None
        while True:
            params: dict[str, Any] = {"limit": PAGE_SIZE}
            if before:
                params["before"] = before
            data = self._client.get_json(path, params=params)
            threads = data.get("threads") or []
            yield from threads
            if not data.get("has_more"):
                return
            next_before = cursor_of(threads[-1]) if threads else None
            if not next_before or next_before == before:
                raise DiscordAPIError(f"Discord pagination of {path} did not advance (cursor {next_before!r})")
            before = next_before

    # ------------------------------------------------------------------
    # Message retrieval
    # ------------------------------------------------------------------

    @staticmethod
    def _is_snowflake(value: Any) -> bool:
        """Return True when the value is a numeric snowflake string."""
        return isinstance(value, str) and bool(SNOWFLAKE_RE.match(value))

    def _iter_messages(self, channel_id: str) -> Iterator[dict]:
        """Walk the full history of a channel, newest first, using the oldest ID as cursor."""
        before: str | None = None
        while True:
            params: dict[str, Any] = {"limit": PAGE_SIZE}
            if before:
                params["before"] = before
            page = self._client.get_json(f"/channels/{channel_id}/messages", params=params)
            if not page:
                return
            if any(not self._is_snowflake(message.get("id")) for message in page):
                raise DiscordAPIError(f"Discord history of channel {channel_id} returned a message without a valid ID")
            yield from page
            if len(page) < PAGE_SIZE:
                return
            next_before = page[-1]["id"]
            if before is not None and int(next_before) >= int(before):
                raise DiscordAPIError(f"Discord history of channel {channel_id} did not advance (cursor {next_before})")
            before = next_before

    def _yield_messages(self, channel: dict, parent: dict | None, *, explicit: bool) -> Iterator[IngestionItem]:
        """Yield items for one channel or thread. Inaccessible discovered channels are skipped."""
        channel_id = channel.get("id")
        previous = self._outcomes.get(channel_id)
        if previous is not None:
            if explicit and previous.error is not None:
                raise previous.error
            return
        outcome = self._outcomes[channel_id] = _ChannelOutcome()
        fetched = 0
        rendered = 0
        empty = 0
        try:
            for message in self._iter_messages(channel_id):
                fetched += 1
                if message.get("type") not in INGESTED_MESSAGE_TYPES:
                    continue
                item = self._build_item(message, channel, parent)
                if item is None:
                    empty += 1
                    continue
                rendered += 1
                outcome.items += 1
                yield item
        except DiscordAPIError as exc:
            if explicit or isinstance(exc, DiscordAuthError) or exc.status not in (403, 404):
                raise
            outcome.error = exc
            self._inaccessible += 1
            logger.warning(f"[{self.source_name}] Skipping inaccessible Discord channel {channel_id}: {exc}")
            return

        if fetched == 0 and channel.get("last_message_id"):
            logger.warning(
                f"[{self.source_name}] Discord channel {channel_id} returned no messages although it has activity. "
                "The bot may lack the Read Message History permission, or all messages were deleted."
            )

        if empty >= EMPTY_MESSAGES_BEFORE_WARNING and empty >= rendered:
            logger.warning(
                f"[{self.source_name}] {empty} of {fetched} message(s) in Discord channel {channel_id} had no "
                "content, attachments or embeds. The Message Content intent may be disabled for the bot."
            )

    def _build_item(self, message: dict, channel: dict, parent: dict | None) -> IngestionItem | None:
        """Build an IngestionItem for a message, or None when it has nothing to index."""
        is_thread = channel.get("type") in THREAD_TYPES
        parent_id = (parent or {}).get("id") or (channel.get("parent_id") if is_thread else None)
        channel_id = parent_id if is_thread else channel.get("id")
        channel_name = self._channel_names.get(channel_id) if channel_id else None
        thread_id = channel.get("id") if is_thread else None
        thread_name = channel.get("name") if is_thread else None

        author = message.get("author") or {}
        author_name = author.get("global_name") or author.get("username") or ""
        text = self._render_message(message, author_name, channel_name, thread_name)
        if not text:
            return None

        guild_id = channel.get("guild_id", "")
        message_id = message.get("id")
        location_id = thread_id or channel_id
        created = message.get("timestamp")
        edited = message.get("edited_timestamp")
        referenced = message.get("message_reference") or {}
        return IngestionItem(
            id=f"discord:{self.source_name}:{message_id}",
            source_ref={
                "guild_id": guild_id,
                "channel_id": channel_id,
                "channel_name": channel_name,
                "thread_id": thread_id,
                "thread_name": thread_name,
                "message_id": message_id,
                "author_id": author.get("id"),
                "author_name": author_name or None,
                "author_is_bot": bool(author.get("bot")),
                "created_at": created,
                "edited_at": edited,
                "reply_to_message_id": referenced.get("message_id") if message.get("type") == 19 else None,
                "url": f"https://discord.com/channels/{guild_id}/{location_id}/{message_id}",
                "text": text,
            },
            last_modified=self._last_modified(edited, created),
        )

    @staticmethod
    def _last_modified(edited: str | None, created: str | None) -> datetime | None:
        """Return the edit time, else the creation time, as a timezone-aware datetime."""
        value = parse_timestamp(edited) or parse_timestamp(created)
        if value is not None and value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def _render_message(
        self, message: dict, author_name: str, channel_name: str | None, thread_name: str | None
    ) -> str:
        """Render a message as text. Returns an empty string when it has nothing to index.

        Attachments are listed by file name only: CDN URLs are signed and change on every fetch,
        which would make every run look like an edit.
        """
        parts = self._message_parts(message)
        for snapshot in message.get("message_snapshots") or []:
            forwarded = self._message_parts((snapshot or {}).get("message") or {})
            if forwarded:
                parts.append("Forwarded message:")
                parts.extend(forwarded)

        if not parts:
            return ""

        location = f"#{channel_name}" if channel_name else "a channel"
        if thread_name:
            location = f"{location} / {thread_name}"
        header = f"{author_name or 'Unknown user'} in {location}"
        replied_to = (message.get("referenced_message") or {}).get("author") or {}
        reply_name = replied_to.get("global_name") or replied_to.get("username")
        if message.get("type") == 19 and reply_name:
            header += f" (reply to {reply_name})"
        return header + ":\n" + "\n".join(parts)

    def _message_parts(self, message: dict) -> list[str]:
        """Return the text, attachment names and embed text of one message or forwarded snapshot."""
        parts: list[str] = []
        content = self._resolve_mentions(message.get("content") or "", message.get("mentions") or [])
        if content.strip():
            parts.append(content.strip())

        names = [a.get("filename") for a in message.get("attachments") or [] if a.get("filename")]
        if names:
            parts.append("Attachments: " + ", ".join(names))

        for embed in message.get("embeds") or []:
            embed_text = " - ".join(v for v in (embed.get("title"), embed.get("description")) if v)
            if embed_text:
                parts.append(f"Embed: {embed_text}")
        return parts

    def _resolve_mentions(self, text: str, mentions: list[dict]) -> str:
        """Replace user and channel mention tokens with names when known. Unknown IDs stay raw."""
        user_names = {u["id"]: u.get("global_name") or u.get("username") for u in mentions if u.get("id")}

        def replace_user(match: re.Match) -> str:
            """Replace a user mention with the user name."""
            name = user_names.get(match.group(1))
            return f"@{name}" if name else match.group(0)

        def replace_channel(match: re.Match) -> str:
            """Replace a channel mention with the channel name."""
            name = self._channel_names.get(match.group(1))
            return f"#{name}" if name else match.group(0)

        text = re.sub(r"<@!?([0-9]+)>", replace_user, text)
        return re.sub(r"<#([0-9]+)>", replace_channel, text)
