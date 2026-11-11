import logging
import unittest
from datetime import UTC, datetime
from unittest.mock import MagicMock, Mock, patch

import requests

from tasks.discord_ingestion import DiscordAPIError, DiscordAuthError, DiscordClient, DiscordIngestionJob
from tasks.factory import IngestionJobFactory

GUILD = "1100000000000000001"
OTHER_GUILD = "1100000000000000002"
GENERAL = "1200000000000000001"
RANDOM = "1200000000000000002"
VOICE = "1200000000000000003"
FORUM = "1200000000000000004"
ANNOUNCE = "1200000000000000005"
THREAD_ACTIVE = "1300000000000000001"
THREAD_ARCHIVED = "1300000000000000002"
THREAD_PRIVATE = "1300000000000000003"
THREAD_OTHER_PARENT = "1300000000000000004"
POST = "1300000000000000005"
BASE_ID = 1400000000000000000

USER_ALICE = {"id": "1500000000000000001", "username": "alice", "global_name": "Alice A."}
USER_BOB = {"id": "1500000000000000002", "username": "bob", "bot": True}


def _msg_id(n):
    return str(BASE_ID + n)


def _message(n, content="Hello world", author=USER_ALICE, msg_type=0, **extra):
    message = {
        "id": _msg_id(n),
        "type": msg_type,
        "content": content,
        "author": author,
        "timestamp": "2024-03-01T10:00:00.123000+00:00",
        "edited_timestamp": None,
        "attachments": [],
        "embeds": [],
        "mentions": [],
    }
    message.update(extra)
    return message


def _channel(channel_id, name, channel_type=0, **extra):
    channel = {"id": channel_id, "type": channel_type, "name": name, "guild_id": GUILD}
    channel.update(extra)
    return channel


def _thread(thread_id, name, parent_id, thread_type=11, archive_timestamp="2024-02-01T00:00:00+00:00"):
    return {
        "id": thread_id,
        "type": thread_type,
        "name": name,
        "guild_id": GUILD,
        "parent_id": parent_id,
        "thread_metadata": {"archived": True, "archive_timestamp": archive_timestamp},
    }


class FakeDiscord:
    """Routes get_json calls to canned responses and records every call."""

    def __init__(self):
        self.routes = {}
        self.calls = []
        self.close_count = 0

    def close(self):
        self.close_count += 1

    def set(self, path, response):
        self.routes[path] = response

    def get_json(self, path, params=None):
        self.calls.append((path, dict(params) if params else None))
        response = self.routes.get(path)
        if response is None:
            raise AssertionError(f"Unexpected Discord request: {path} {params}")
        if isinstance(response, Exception):
            raise response
        if callable(response):
            return response(params or {})
        return response

    def paths(self):
        return [path for path, _ in self.calls]


def _make_config(name="discord1", **config):
    base = {"bot_token": "token", "guild_ids": [GUILD], "channel_ids": []}
    base.update(config)
    return {"name": name, "config": base}


class TestDiscordIngestionJob(unittest.TestCase):
    def setUp(self):
        patcher = patch("tasks.discord_ingestion.DiscordClient")
        self.mock_client_class = patcher.start()
        self.addCleanup(patcher.stop)
        self.discord = FakeDiscord()
        self.mock_client_class.return_value = self.discord

    def _make_job(self, **kwargs):
        return DiscordIngestionJob(_make_config(**kwargs))

    def _guild_with_general(self, messages=None, active=None):
        self.discord.set(
            f"/guilds/{GUILD}/channels",
            [
                _channel(GENERAL, "general"),
                _channel(VOICE, "Lounge", channel_type=2),
                {"id": "1200000000000000009", "type": 4, "name": "Category", "guild_id": GUILD},
            ],
        )
        self.discord.set(f"/channels/{GENERAL}/messages", messages if messages is not None else [])
        self.discord.set(f"/guilds/{GUILD}/threads/active", {"threads": active or []})
        self._no_archives(GENERAL)

    def _no_archives(self, channel_id, public=None):
        self.discord.set(
            f"/channels/{channel_id}/threads/archived/public", public or {"threads": [], "has_more": False}
        )
        self.discord.set(
            f"/channels/{channel_id}/users/@me/threads/archived/private", {"threads": [], "has_more": False}
        )
        self.discord.set(f"/channels/{channel_id}/threads/archived/private", {"threads": [], "has_more": False})

    def _items(self, job):
        return list(job.list_items())

    def test_source_type(self):
        self.assertEqual(self._make_job().source_type, "discord")

    def test_client_is_closed_once_after_list_items_completes(self):
        self._guild_with_general(messages=[_message(1)])
        job = self._make_job()
        self.assertEqual(self.discord.close_count, 0)
        self._items(job)
        self.assertEqual(self.discord.close_count, 1)

    def test_client_is_closed_when_list_items_fails(self):
        self.discord.set(f"/guilds/{GUILD}/channels", DiscordAPIError("boom", status=500))
        with self.assertRaises(DiscordAPIError):
            self._items(self._make_job())
        self.assertEqual(self.discord.close_count, 1)

    def test_missing_or_blank_token_raises(self):
        for token in (None, "", "   "):
            with self.assertRaises(ValueError):
                self._make_job(bot_token=token)

    def test_requires_at_least_one_selector(self):
        with self.assertRaises(ValueError):
            self._make_job(guild_ids="", channel_ids=None)

    def test_invalid_ids_raise(self):
        for bad in ("abc", "1.2e+18", "12 34", "-5"):
            with self.assertRaises(ValueError, msg=bad):
                self._make_job(guild_ids=bad)
        with self.assertRaises(ValueError):
            self._make_job(guild_ids=[], channel_ids=["not-an-id"])

    def test_id_lists_accept_yaml_lists_and_env_strings_without_precision_loss(self):
        from_yaml = self._make_job(guild_ids=[GUILD, int(OTHER_GUILD)], channel_ids=[GENERAL, GENERAL])
        from_env = self._make_job(guild_ids=f" {GUILD} , {OTHER_GUILD} ", channel_ids=f"{GENERAL},{GENERAL}")
        self.assertEqual(from_yaml.guild_ids, [GUILD, OTHER_GUILD])
        self.assertEqual(from_env.guild_ids, [GUILD, OTHER_GUILD])
        self.assertEqual(from_yaml.channel_ids, [GENERAL])
        self.assertEqual(from_env.channel_ids, [GENERAL])

    def test_factory_creates_discord_job(self):
        job = IngestionJobFactory.create("discord", _make_config())
        self.assertIsInstance(job, DiscordIngestionJob)

    def test_guild_only_discovers_message_and_thread_channels_but_not_voice_or_category(self):
        self._guild_with_general(messages=[_message(1)])
        job = self._make_job()
        items = self._items(job)
        self.assertEqual(len(items), 1)
        self.assertNotIn(f"/channels/{VOICE}/messages", self.discord.paths())

    def test_channel_only_resolves_only_listed_channels(self):
        self.discord.set(f"/channels/{GENERAL}", _channel(GENERAL, "general"))
        self.discord.set(f"/channels/{GENERAL}/messages", [_message(1)])
        self.discord.set(f"/guilds/{GUILD}/threads/active", {"threads": []})
        self._no_archives(GENERAL)
        job = self._make_job(guild_ids=[], channel_ids=[GENERAL])
        self.assertEqual(len(self._items(job)), 1)
        self.assertNotIn(f"/guilds/{GUILD}/channels", self.discord.paths())

    def test_both_selectors_restrict_to_listed_channels(self):
        self.discord.set(f"/channels/{GENERAL}", _channel(GENERAL, "general"))
        self.discord.set(f"/channels/{GENERAL}/messages", [_message(1)])
        self.discord.set(f"/guilds/{GUILD}/threads/active", {"threads": []})
        self._no_archives(GENERAL)
        job = self._make_job(guild_ids=[GUILD], channel_ids=[GENERAL])
        self.assertEqual(len(self._items(job)), 1)
        self.assertNotIn(f"/guilds/{GUILD}/channels", self.discord.paths())

    def test_channel_from_unlisted_guild_raises(self):
        self.discord.set(f"/channels/{GENERAL}", _channel(GENERAL, "general", guild_id=OTHER_GUILD))
        job = self._make_job(guild_ids=[GUILD], channel_ids=[GENERAL])
        with self.assertRaisesRegex(ValueError, "not in guild_ids"):
            self._items(job)

    def test_unsupported_channel_type_raises(self):
        self.discord.set(f"/channels/{VOICE}", _channel(VOICE, "Lounge", channel_type=2))
        job = self._make_job(guild_ids=[], channel_ids=[VOICE])
        with self.assertRaisesRegex(ValueError, "unsupported type"):
            self._items(job)

    def test_explicit_channel_not_found_fails_instead_of_returning_nothing(self):
        self.discord.set(f"/channels/{GENERAL}", DiscordAPIError("Unknown Channel", status=404))
        job = self._make_job(guild_ids=[], channel_ids=[GENERAL])
        with self.assertRaises(DiscordAPIError):
            self._items(job)

    def test_explicit_channel_history_denied_fails(self):
        self.discord.set(f"/channels/{GENERAL}", _channel(GENERAL, "general"))
        self.discord.set(f"/channels/{GENERAL}/messages", DiscordAPIError("Missing Access", status=403))
        job = self._make_job(guild_ids=[], channel_ids=[GENERAL])
        with self.assertRaises(DiscordAPIError):
            self._items(job)

    def test_discovered_channel_history_denied_is_skipped_with_warning(self):
        self.discord.set(f"/guilds/{GUILD}/channels", [_channel(GENERAL, "general"), _channel(RANDOM, "random")])
        self.discord.set(f"/channels/{GENERAL}/messages", DiscordAPIError("Missing Access", status=403))
        self.discord.set(f"/channels/{RANDOM}/messages", [_message(1)])
        self.discord.set(f"/guilds/{GUILD}/threads/active", {"threads": []})
        self._no_archives(GENERAL)
        self._no_archives(RANDOM)
        job = self._make_job()
        with self.assertLogs("tasks.discord_ingestion", level="WARNING") as logs:
            items = self._items(job)
        self.assertEqual([i.source_ref["channel_id"] for i in items], [RANDOM])
        self.assertTrue(any("inaccessible" in line for line in logs.output))

    def test_invalid_token_during_discovery_propagates(self):
        self.discord.set(f"/guilds/{GUILD}/channels", DiscordAuthError("bad token", status=401))
        with self.assertRaises(DiscordAuthError):
            self._items(self._make_job())

    def test_unreachable_guild_fails(self):
        self.discord.set(f"/guilds/{GUILD}/channels", DiscordAPIError("Missing Access", status=403))
        with self.assertRaises(DiscordAPIError):
            self._items(self._make_job())

    def test_inaccessible_guild_is_skipped_when_another_guild_works(self):
        self.discord.set(f"/guilds/{OTHER_GUILD}/channels", DiscordAPIError("Missing Access", status=403))
        self._guild_with_general(messages=[_message(1)])
        job = self._make_job(guild_ids=[OTHER_GUILD, GUILD])
        with self.assertLogs("tasks.discord_ingestion", level="WARNING") as logs:
            items = self._items(job)
        self.assertEqual(len(items), 1)
        self.assertTrue(any(OTHER_GUILD in line for line in logs.output))

    def test_all_guilds_inaccessible_fails(self):
        self.discord.set(f"/guilds/{OTHER_GUILD}/channels", DiscordAPIError("Missing Access", status=403))
        self.discord.set(f"/guilds/{GUILD}/channels", DiscordAPIError("Unknown Guild", status=404))
        with self.assertRaises(DiscordAPIError):
            self._items(self._make_job(guild_ids=[OTHER_GUILD, GUILD]))

    def test_explicit_channel_with_empty_history_but_activity_warns_without_failing(self):
        self.discord.set(f"/channels/{GENERAL}", _channel(GENERAL, "general", last_message_id=_msg_id(9)))
        self.discord.set(f"/channels/{GENERAL}/messages", [])
        self.discord.set(f"/guilds/{GUILD}/threads/active", {"threads": []})
        self._no_archives(GENERAL)
        job = self._make_job(guild_ids=[], channel_ids=[GENERAL])
        with self.assertLogs("tasks.discord_ingestion", level="WARNING") as logs:
            self.assertEqual(self._items(job), [])
        self.assertTrue(any("Read Message History" in line for line in logs.output))

    def test_explicit_thread_with_empty_history_but_activity_warns_without_failing(self):
        self.discord.set(
            f"/channels/{THREAD_ACTIVE}", {**_thread(THREAD_ACTIVE, "Plan", GENERAL), "last_message_id": _msg_id(9)}
        )
        self.discord.set(f"/channels/{GENERAL}", _channel(GENERAL, "general"))
        self.discord.set(f"/channels/{THREAD_ACTIVE}/messages", [])
        job = self._make_job(guild_ids=[], channel_ids=[THREAD_ACTIVE])
        with self.assertLogs("tasks.discord_ingestion", level="WARNING") as logs:
            self.assertEqual(self._items(job), [])
        self.assertTrue(any("Read Message History" in line for line in logs.output))

    def test_discovered_channel_with_empty_history_but_activity_warns_and_continues(self):
        self.discord.set(
            f"/guilds/{GUILD}/channels",
            [_channel(GENERAL, "general", last_message_id=_msg_id(9)), _channel(RANDOM, "random")],
        )
        self.discord.set(f"/channels/{GENERAL}/messages", [])
        self.discord.set(f"/channels/{RANDOM}/messages", [_message(1)])
        self.discord.set(f"/guilds/{GUILD}/threads/active", {"threads": []})
        self._no_archives(GENERAL)
        self._no_archives(RANDOM)
        with self.assertLogs("tasks.discord_ingestion", level="WARNING") as logs:
            items = self._items(self._make_job())
        self.assertEqual(len(items), 1)
        self.assertTrue(any("Read Message History" in line for line in logs.output))

    def test_short_history_page_with_message_without_id_raises(self):
        page = [_message(3), _message(2), _message(1)]
        del page[1]["id"]
        self._guild_with_general()
        self.discord.set(f"/channels/{GENERAL}/messages", lambda params: page)
        with self.assertRaisesRegex(DiscordAPIError, "without a valid ID"):
            next(iter(self._make_job().list_items()))

    def test_history_message_without_id_raises_descriptive_error(self):
        page = [_message(1000 - n) for n in range(100)]
        del page[-1]["id"]
        self._guild_with_general()
        self.discord.set(f"/channels/{GENERAL}/messages", lambda params: page)
        with self.assertRaisesRegex(DiscordAPIError, "without a valid ID"):
            next(iter(self._make_job().list_items()))

    def test_empty_channel_yields_nothing_without_error(self):
        self._guild_with_general(messages=[])
        self.assertEqual(self._items(self._make_job()), [])

    def test_history_walks_every_page_using_oldest_id_as_cursor(self):
        first_page = [_message(1000 - n) for n in range(100)]
        second_page = [_message(900 - n) for n in range(3)]
        pages = {None: first_page, first_page[-1]["id"]: second_page}
        self._guild_with_general()
        self.discord.set(f"/channels/{GENERAL}/messages", lambda params: pages[params.get("before")])
        items = self._items(self._make_job())
        self.assertEqual(len(items), 103)
        history_calls = [p for path, p in self.discord.calls if path == f"/channels/{GENERAL}/messages"]
        self.assertEqual(history_calls, [{"limit": 100}, {"limit": 100, "before": _msg_id(901)}])

    def test_history_with_non_advancing_cursor_raises(self):
        page = [_message(1000 - n) for n in range(100)]
        self._guild_with_general()
        self.discord.set(f"/channels/{GENERAL}/messages", lambda params: page)
        with self.assertRaisesRegex(DiscordAPIError, "did not advance"):
            self._items(self._make_job())

    def test_active_thread_replies_are_read_even_when_parent_history_is_empty(self):
        thread = _thread(THREAD_ACTIVE, "Release plan", GENERAL, archive_timestamp=None)
        other = _thread(THREAD_OTHER_PARENT, "Elsewhere", RANDOM)
        self._guild_with_general(messages=[], active=[thread, other])
        self.discord.set(f"/channels/{THREAD_ACTIVE}/messages", [_message(5, "newer reply")])
        items = self._items(self._make_job())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].source_ref["thread_id"], THREAD_ACTIVE)
        self.assertEqual(items[0].source_ref["channel_id"], GENERAL)
        self.assertNotIn(f"/channels/{THREAD_OTHER_PARENT}/messages", self.discord.paths())

    def test_archived_public_threads_are_paged_with_archive_timestamp_cursor(self):
        first = _thread(THREAD_ARCHIVED, "Old topic", GENERAL, archive_timestamp="2024-01-05T00:00:00+00:00")
        second = _thread(POST, "Older topic", GENERAL, archive_timestamp="2023-12-01T00:00:00+00:00")
        pages = {
            None: {"threads": [first], "has_more": True},
            "2024-01-05T00:00:00+00:00": {"threads": [second], "has_more": False},
        }
        self._guild_with_general()
        self.discord.set(f"/channels/{GENERAL}/threads/archived/public", lambda params: pages[params.get("before")])
        self.discord.set(f"/channels/{THREAD_ARCHIVED}/messages", [_message(10, "from first")])
        self.discord.set(f"/channels/{POST}/messages", [_message(11, "from second")])
        items = self._items(self._make_job())
        self.assertEqual({i.source_ref["thread_id"] for i in items}, {THREAD_ARCHIVED, POST})

    def test_joined_private_archive_uses_thread_id_cursor(self):
        private_a = _thread(THREAD_PRIVATE, "Private A", GENERAL, thread_type=12)
        private_b = _thread(THREAD_OTHER_PARENT, "Private B", GENERAL, thread_type=12)
        pages = {
            None: {"threads": [private_a], "has_more": True},
            THREAD_PRIVATE: {"threads": [private_b], "has_more": False},
        }
        self._guild_with_general()
        self.discord.set(
            f"/channels/{GENERAL}/users/@me/threads/archived/private", lambda params: pages[params.get("before")]
        )
        self.discord.set(f"/channels/{THREAD_PRIVATE}/messages", [_message(20)])
        self.discord.set(f"/channels/{THREAD_OTHER_PARENT}/messages", [_message(21)])
        items = self._items(self._make_job())
        self.assertEqual({i.source_ref["thread_id"] for i in items}, {THREAD_PRIVATE, THREAD_OTHER_PARENT})

    def test_private_archive_without_manage_threads_is_reported_not_fatal(self):
        self._guild_with_general(messages=[_message(1)])
        self.discord.set(
            f"/channels/{GENERAL}/threads/archived/private", DiscordAPIError("Missing Permissions", status=403)
        )
        job = self._make_job()
        with self.assertLogs("tasks.discord_ingestion", level="WARNING") as logs:
            items = self._items(job)
        self.assertEqual(len(items), 1)
        self.assertTrue(any("private archived threads" in line for line in logs.output))

    def test_archive_listing_that_does_not_advance_raises(self):
        thread = _thread(THREAD_ARCHIVED, "Old", GENERAL, archive_timestamp="2024-01-05T00:00:00+00:00")
        self._guild_with_general()
        self.discord.set(
            f"/channels/{GENERAL}/threads/archived/public", lambda params: {"threads": [thread], "has_more": True}
        )
        self.discord.set(f"/channels/{THREAD_ARCHIVED}/messages", [_message(10)])
        with self.assertRaisesRegex(DiscordAPIError, "did not advance"):
            self._items(self._make_job())

    def test_thread_found_by_two_paths_is_read_once(self):
        thread = _thread(THREAD_ACTIVE, "Both", GENERAL)
        self._guild_with_general(active=[thread])
        self.discord.set(f"/channels/{GENERAL}/threads/archived/public", {"threads": [thread], "has_more": False})
        self.discord.set(f"/channels/{THREAD_ACTIVE}/messages", [_message(5)])
        items = self._items(self._make_job())
        self.assertEqual(len(items), 1)
        self.assertEqual(self.discord.paths().count(f"/channels/{THREAD_ACTIVE}/messages"), 1)

    def test_thread_selected_directly_and_through_its_parent_is_read_once(self):
        thread = _thread(THREAD_ACTIVE, "Release plan", GENERAL, archive_timestamp=None)
        for channel_ids in ([THREAD_ACTIVE, GENERAL], [GENERAL, THREAD_ACTIVE]):
            self.discord = FakeDiscord()
            self.mock_client_class.return_value = self.discord
            self.discord.set(f"/channels/{GENERAL}", _channel(GENERAL, "general"))
            self.discord.set(f"/channels/{THREAD_ACTIVE}", thread)
            self.discord.set(f"/channels/{GENERAL}/messages", [_message(1)])
            self.discord.set(f"/channels/{THREAD_ACTIVE}/messages", [_message(2)])
            self.discord.set(f"/guilds/{GUILD}/threads/active", {"threads": [thread]})
            self._no_archives(GENERAL)
            items = self._items(self._make_job(guild_ids=[], channel_ids=channel_ids))
            self.assertEqual(sorted(i.source_ref["message_id"] for i in items), [_msg_id(1), _msg_id(2)])
            self.assertEqual(self.discord.paths().count(f"/channels/{THREAD_ACTIVE}/messages"), 1)

    def test_private_archive_denied_for_one_channel_does_not_hide_another_channels_archive(self):
        private = _thread(THREAD_PRIVATE, "Private", RANDOM, thread_type=12)
        self.discord.set(f"/guilds/{GUILD}/channels", [_channel(GENERAL, "general"), _channel(RANDOM, "random")])
        self.discord.set(f"/guilds/{GUILD}/threads/active", {"threads": []})
        for channel_id in (GENERAL, RANDOM):
            self.discord.set(f"/channels/{channel_id}/messages", [])
            self._no_archives(channel_id)
        self.discord.set(
            f"/channels/{GENERAL}/threads/archived/private", DiscordAPIError("Missing Permissions", status=403)
        )
        self.discord.set(f"/channels/{RANDOM}/threads/archived/private", {"threads": [private], "has_more": False})
        self.discord.set(f"/channels/{THREAD_PRIVATE}/messages", [_message(7, "private note")])
        with self.assertLogs("tasks.discord_ingestion", level="WARNING"):
            items = self._items(self._make_job())
        self.assertEqual([i.source_ref["thread_id"] for i in items], [THREAD_PRIVATE])

    def _emptied_parent_with_surviving_thread(self, channel_ids):
        thread = _thread(THREAD_ACTIVE, "Survivor", GENERAL, archive_timestamp=None)
        self.discord = FakeDiscord()
        self.mock_client_class.return_value = self.discord
        self.discord.set(f"/channels/{GENERAL}", _channel(GENERAL, "general", last_message_id=_msg_id(9)))
        self.discord.set(f"/channels/{THREAD_ACTIVE}", thread)
        self.discord.set(f"/channels/{GENERAL}/messages", [])
        self.discord.set(f"/channels/{THREAD_ACTIVE}/messages", [_message(5, "still here")])
        self.discord.set(f"/guilds/{GUILD}/threads/active", {"threads": [thread]})
        self._no_archives(GENERAL)
        return self._make_job(guild_ids=[], channel_ids=channel_ids)

    def test_emptied_parent_does_not_fail_when_its_thread_was_already_read_directly(self):
        for channel_ids in ([THREAD_ACTIVE, GENERAL], [GENERAL, THREAD_ACTIVE]):
            job = self._emptied_parent_with_surviving_thread(channel_ids)
            with self.assertLogs("tasks.discord_ingestion", level="WARNING"):
                items = self._items(job)
            self.assertEqual([i.source_ref["thread_id"] for i in items], [THREAD_ACTIVE], channel_ids)

    def test_thread_failure_while_discovered_is_raised_when_the_thread_is_also_selected_explicitly(self):
        for channel_ids in ([GENERAL, THREAD_ACTIVE], [THREAD_ACTIVE, GENERAL]):
            thread = _thread(THREAD_ACTIVE, "Locked", GENERAL, archive_timestamp=None)
            self.discord = FakeDiscord()
            self.mock_client_class.return_value = self.discord
            self.discord.set(f"/channels/{GENERAL}", _channel(GENERAL, "general"))
            self.discord.set(f"/channels/{THREAD_ACTIVE}", thread)
            self.discord.set(f"/channels/{GENERAL}/messages", [_message(1)])
            self.discord.set(f"/channels/{THREAD_ACTIVE}/messages", DiscordAPIError("Missing Access", status=403))
            self.discord.set(f"/guilds/{GUILD}/threads/active", {"threads": [thread]})
            self._no_archives(GENERAL)
            job = self._make_job(guild_ids=[], channel_ids=channel_ids)
            with self.assertRaises(DiscordAPIError, msg=channel_ids):
                self._items(job)

    def test_readable_emptied_parent_still_reads_its_threads(self):
        thread = _thread(THREAD_ACTIVE, "Survivor", GENERAL, archive_timestamp=None)
        self.discord.set(f"/channels/{GENERAL}", _channel(GENERAL, "general", last_message_id=_msg_id(9)))
        self.discord.set(f"/channels/{GENERAL}/messages", [])
        self.discord.set(f"/channels/{THREAD_ACTIVE}/messages", [_message(5, "still here")])
        self.discord.set(f"/guilds/{GUILD}/threads/active", {"threads": [thread]})
        self._no_archives(GENERAL)
        job = self._make_job(guild_ids=[], channel_ids=[GENERAL])
        with self.assertLogs("tasks.discord_ingestion", level="WARNING") as logs:
            items = self._items(job)
        self.assertEqual([i.source_ref["thread_id"] for i in items], [THREAD_ACTIVE])
        self.assertTrue(any("Read Message History" in line for line in logs.output))

    def test_malformed_non_numeric_message_ids_raise_before_anything_is_yielded(self):
        short_page = [_message(3), _message(2), _message(1)]
        short_page[1]["id"] = "abc"
        full_page = [_message(1000 - n) for n in range(100)]
        full_page[-1]["id"] = "not-a-snowflake"
        for page in (short_page, full_page):
            self._guild_with_general()
            self.discord.set(f"/channels/{GENERAL}/messages", lambda params, page=page: page)
            with self.assertRaisesRegex(DiscordAPIError, "without a valid ID"):
                next(iter(self._make_job().list_items()))

    def _explicit_forum(self, public_archive, posts_history):
        post = _thread(POST, "How do I deploy?", FORUM, archive_timestamp=None)
        self.discord.set(f"/channels/{FORUM}", _channel(FORUM, "help-forum", channel_type=15, last_message_id=POST))
        self.discord.set(f"/guilds/{GUILD}/threads/active", {"threads": [{**post, "last_message_id": _msg_id(9)}]})
        self.discord.set(f"/channels/{FORUM}/threads/archived/public", public_archive)
        self.discord.set(f"/channels/{POST}/messages", posts_history)
        return self._make_job(guild_ids=[], channel_ids=[FORUM])

    def test_explicit_forum_fails_when_archived_posts_cannot_be_listed(self):
        job = self._explicit_forum(DiscordAPIError("Missing Access", status=403), [_message(30, "post body")])
        with self.assertRaises(DiscordAPIError):
            self._items(job)

    def test_explicit_forum_with_empty_post_history_warns_without_failing(self):
        job = self._explicit_forum({"threads": [], "has_more": False}, [])
        with self.assertLogs("tasks.discord_ingestion", level="WARNING"):
            self.assertEqual(self._items(job), [])

    def test_explicit_forum_with_readable_posts_succeeds(self):
        job = self._explicit_forum({"threads": [], "has_more": False}, [_message(30, "post body")])
        self.assertEqual(len(self._items(job)), 1)

    def test_discovered_forum_with_unlistable_archive_only_warns(self):
        self.discord.set(f"/guilds/{GUILD}/channels", [_channel(FORUM, "help-forum", channel_type=15)])
        self.discord.set(f"/guilds/{GUILD}/threads/active", {"threads": []})
        self.discord.set(f"/channels/{FORUM}/threads/archived/public", DiscordAPIError("Missing Access", status=403))
        with self.assertLogs("tasks.discord_ingestion", level="WARNING"):
            self.assertEqual(self._items(self._make_job()), [])

    def test_forum_posts_are_read_but_forum_has_no_message_history(self):
        post = _thread(POST, "How do I deploy?", FORUM, archive_timestamp=None)
        self.discord.set(f"/guilds/{GUILD}/channels", [_channel(FORUM, "help-forum", channel_type=15)])
        self.discord.set(f"/guilds/{GUILD}/threads/active", {"threads": [post]})
        self.discord.set(f"/channels/{FORUM}/threads/archived/public", {"threads": [], "has_more": False})
        self.discord.set(f"/channels/{POST}/messages", [_message(30, "First post body")])
        items = self._items(self._make_job())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].source_ref["thread_name"], "How do I deploy?")
        self.assertNotIn(f"/channels/{FORUM}/messages", self.discord.paths())
        self.assertNotIn(f"/channels/{FORUM}/users/@me/threads/archived/private", self.discord.paths())

    def test_directly_selected_thread_is_read_without_thread_discovery(self):
        self.discord.set(f"/channels/{THREAD_ACTIVE}", _thread(THREAD_ACTIVE, "Release plan", GENERAL))
        self.discord.set(f"/channels/{GENERAL}", _channel(GENERAL, "general"))
        self.discord.set(f"/channels/{THREAD_ACTIVE}/messages", [_message(5, "Ship it")])
        job = self._make_job(guild_ids=[], channel_ids=[THREAD_ACTIVE])
        items = self._items(job)
        self.assertEqual(items[0].source_ref["thread_id"], THREAD_ACTIVE)
        self.assertEqual(items[0].source_ref["channel_id"], GENERAL)
        self.assertEqual(items[0].source_ref["channel_name"], "general")
        self.assertEqual(job.get_raw_content(items[0]), "Alice A. in #general / Release plan:\nShip it")
        self.assertEqual(
            self.discord.paths(),
            [f"/channels/{THREAD_ACTIVE}", f"/channels/{GENERAL}", f"/channels/{THREAD_ACTIVE}/messages"],
        )

    def test_directly_selected_thread_with_unreadable_parent_still_ingests_without_parent_name(self):
        self.discord.set(f"/channels/{THREAD_ACTIVE}", _thread(THREAD_ACTIVE, "Release plan", GENERAL))
        self.discord.set(f"/channels/{GENERAL}", DiscordAPIError("Missing Access", status=403))
        self.discord.set(f"/channels/{THREAD_ACTIVE}/messages", [_message(5, "Ship it")])
        job = self._make_job(guild_ids=[], channel_ids=[THREAD_ACTIVE])
        with self.assertLogs("tasks.discord_ingestion", level="WARNING"):
            items = self._items(job)
        self.assertEqual(items[0].source_ref["channel_name"], None)
        self.assertEqual(items[0].source_ref["channel_id"], GENERAL)

    def test_system_messages_are_skipped_and_replies_are_kept(self):
        messages = [
            _message(1, "Alice joined", msg_type=7),
            _message(2, "", msg_type=21),
            _message(
                3,
                "I agree",
                author=USER_BOB,
                msg_type=19,
                message_reference={"message_id": _msg_id(0), "channel_id": GENERAL},
                referenced_message=_message(0, "Original", author=USER_ALICE),
            ),
        ]
        self._guild_with_general(messages=messages)
        job = self._make_job()
        items = self._items(job)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].source_ref["reply_to_message_id"], _msg_id(0))
        self.assertEqual(job.get_raw_content(items[0]), "bob in #general (reply to Alice A.):\nI agree")

    def test_application_command_responses_are_kept(self):
        self._guild_with_general(
            messages=[_message(1, "Slash reply", msg_type=20), _message(2, "Menu reply", msg_type=23)]
        )
        job = self._make_job()
        items = self._items(job)
        self.assertEqual(len(items), 2)
        self.assertEqual(job.get_raw_content(items[0]), "Alice A. in #general:\nSlash reply")

    def test_forwarded_message_renders_snapshot_content_under_forwarding_id(self):
        snapshot = {
            "message": {
                "content": "Original text",
                "attachments": [{"filename": "spec.pdf"}],
                "embeds": [{"title": "Doc", "description": "Summary"}],
            }
        }
        self._guild_with_general(messages=[_message(1, "", message_snapshots=[snapshot])])
        job = self._make_job()
        items = self._items(job)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].source_ref["message_id"], _msg_id(1))
        self.assertEqual(
            job.get_raw_content(items[0]),
            "Alice A. in #general:\nForwarded message:\nOriginal text\nAttachments: spec.pdf\nEmbed: Doc - Summary",
        )

    def test_messages_without_content_attachments_or_embeds_are_skipped(self):
        self._guild_with_general(messages=[_message(1, ""), _message(2, "   ")])
        self.assertEqual(self._items(self._make_job()), [])

    def test_many_empty_messages_warn_about_message_content_intent(self):
        self._guild_with_general(messages=[_message(n, "") for n in range(1, 8)])
        with self.assertLogs("tasks.discord_ingestion", level="WARNING") as logs:
            self.assertEqual(self._items(self._make_job()), [])
        self.assertTrue(any("Message Content intent" in line for line in logs.output))

    def test_mostly_empty_history_warns_even_when_some_messages_render(self):
        messages = [_message(n, "") for n in range(1, 7)] + [_message(10, "real"), _message(11, "also real")]
        self._guild_with_general(messages=messages)
        with self.assertLogs("tasks.discord_ingestion", level="WARNING") as logs:
            self.assertEqual(len(self._items(self._make_job())), 2)
        self.assertTrue(any("Message Content intent" in line for line in logs.output))

    def test_few_empty_messages_among_many_do_not_warn(self):
        messages = [_message(n, "") for n in range(1, 7)] + [_message(100 + n, "real") for n in range(20)]
        self._guild_with_general(messages=messages)
        with self.assertNoLogs("tasks.discord_ingestion", level=logging.WARNING):
            self.assertEqual(len(self._items(self._make_job())), 20)

    def test_single_empty_message_does_not_warn_about_intent(self):
        self._guild_with_general(messages=[_message(1, ""), _message(2, "real")])
        with self.assertNoLogs("tasks.discord_ingestion", level=logging.WARNING):
            self.assertEqual(len(self._items(self._make_job())), 1)

    def test_raw_content_renders_author_location_and_text(self):
        self._guild_with_general(messages=[_message(1, "Hello world")])
        job = self._make_job()
        item = self._items(job)[0]
        self.assertEqual(job.get_raw_content(item), "Alice A. in #general:\nHello world")

    def test_username_is_used_when_global_name_is_missing(self):
        author = {"id": "1500000000000000009", "username": "carol"}
        self._guild_with_general(messages=[_message(1, "Hi", author=author)])
        job = self._make_job()
        self.assertTrue(job.get_raw_content(self._items(job)[0]).startswith("carol in #general:"))

    def test_thread_message_content_names_parent_and_thread(self):
        thread = _thread(THREAD_ACTIVE, "Release plan", GENERAL, archive_timestamp=None)
        self._guild_with_general(active=[thread])
        self.discord.set(f"/channels/{THREAD_ACTIVE}/messages", [_message(5, "Ship it")])
        job = self._make_job()
        self.assertEqual(job.get_raw_content(self._items(job)[0]), "Alice A. in #general / Release plan:\nShip it")

    def test_mentions_are_resolved_and_unknown_ids_stay_raw(self):
        content = f"Hi <@{USER_BOB['id']}> and <@!{USER_ALICE['id']}>, see <#{RANDOM}> <#999> and <@&777> <@888>"
        message = _message(1, content, mentions=[USER_BOB, USER_ALICE])
        self.discord.set(f"/guilds/{GUILD}/channels", [_channel(GENERAL, "general"), _channel(RANDOM, "random")])
        self.discord.set(f"/channels/{GENERAL}/messages", [message])
        self.discord.set(f"/channels/{RANDOM}/messages", [])
        self.discord.set(f"/guilds/{GUILD}/threads/active", {"threads": []})
        self._no_archives(GENERAL)
        self._no_archives(RANDOM)
        job = self._make_job()
        self.assertEqual(
            job.get_raw_content(self._items(job)[0]),
            "Alice A. in #general:\nHi @bob and @Alice A., see #random <#999> and <@&777> <@888>",
        )

    def test_attachments_are_listed_by_name_and_embeds_by_text_without_urls(self):
        message = _message(
            1,
            "",
            attachments=[
                {"filename": "plan.pdf", "url": "https://cdn.discordapp.com/x?ex=1&hm=abc"},
                {"filename": "chart.png"},
            ],
            embeds=[{"title": "Docs", "description": "Read this"}, {"url": "https://example.com"}],
        )
        self._guild_with_general(messages=[message])
        job = self._make_job()
        text = job.get_raw_content(self._items(job)[0])
        self.assertEqual(text, "Alice A. in #general:\nAttachments: plan.pdf, chart.png\nEmbed: Docs - Read this")
        self.assertNotIn("cdn.discordapp.com", text)

    def test_extra_metadata_for_channel_message(self):
        self._guild_with_general(messages=[_message(1, "Hello", edited_timestamp="2024-03-02T08:30:00+00:00")])
        job = self._make_job()
        item = self._items(job)[0]
        self.assertEqual(
            job.get_extra_metadata(item, "", {}),
            {
                "guild_id": GUILD,
                "channel_id": GENERAL,
                "channel_name": "general",
                "thread_id": None,
                "thread_name": None,
                "message_id": _msg_id(1),
                "author_id": USER_ALICE["id"],
                "author_name": "Alice A.",
                "author_is_bot": False,
                "created_at": "2024-03-01T10:00:00.123000+00:00",
                "edited_at": "2024-03-02T08:30:00+00:00",
                "reply_to_message_id": None,
                "url": f"https://discord.com/channels/{GUILD}/{GENERAL}/{_msg_id(1)}",
            },
        )

    def test_extra_metadata_for_thread_message_keeps_parent_and_thread_distinct(self):
        thread = _thread(THREAD_ACTIVE, "Release plan", GENERAL, archive_timestamp=None)
        self._guild_with_general(active=[thread])
        self.discord.set(f"/channels/{THREAD_ACTIVE}/messages", [_message(5, author=USER_BOB)])
        job = self._make_job()
        extra = job.get_extra_metadata(self._items(job)[0], "", {})
        self.assertEqual(extra["channel_id"], GENERAL)
        self.assertEqual(extra["channel_name"], "general")
        self.assertEqual(extra["thread_id"], THREAD_ACTIVE)
        self.assertEqual(extra["thread_name"], "Release plan")
        self.assertTrue(extra["author_is_bot"])
        self.assertEqual(extra["url"], f"https://discord.com/channels/{GUILD}/{THREAD_ACTIVE}/{_msg_id(5)}")

    def test_extra_metadata_does_not_use_reserved_keys(self):
        self._guild_with_general(messages=[_message(1)])
        job = self._make_job()
        extra = job.get_extra_metadata(self._items(job)[0], "", {})
        reserved = {"source", "key", "checksum", "version", "format", "source_name", "file_name", "last_modified"}
        self.assertFalse(reserved & set(extra))

    def test_last_modified_is_edit_time_else_creation_time_and_timezone_aware(self):
        edited = _message(1, edited_timestamp="2024-03-02T08:30:00+00:00")
        created_only = _message(2)
        self._guild_with_general(messages=[edited, created_only])
        items = {i.source_ref["message_id"]: i for i in self._items(self._make_job())}
        self.assertEqual(items[_msg_id(1)].last_modified, datetime(2024, 3, 2, 8, 30, tzinfo=UTC))
        self.assertEqual(items[_msg_id(2)].last_modified, datetime(2024, 3, 1, 10, 0, 0, 123000, tzinfo=UTC))

    def test_last_modified_is_none_when_message_has_no_timestamps(self):
        message = _message(1, timestamp=None)
        self._guild_with_general(messages=[message])
        self.assertIsNone(self._items(self._make_job())[0].last_modified)

    def test_item_name_is_built_from_ids_and_is_unique_per_message_and_thread(self):
        thread = _thread(THREAD_ACTIVE, "Release plan", GENERAL, archive_timestamp=None)
        self._guild_with_general(messages=[_message(1), _message(2)], active=[thread])
        self.discord.set(f"/channels/{THREAD_ACTIVE}/messages", [_message(3)])
        job = self._make_job()
        names = [job.get_item_name(i) for i in self._items(job)]
        self.assertEqual(len(set(names)), 3)
        self.assertTrue(all(f"_{GUILD}_" in n for n in names))
        self.assertTrue(any(f"_{THREAD_ACTIVE}_{_msg_id(3)}" in n for n in names))

    def test_item_names_differ_between_sources_with_colliding_sanitized_names(self):
        self._guild_with_general(messages=[_message(1)])
        names = []
        for source_name in ("team/support", "team_support", "team support"):
            job = self._make_job(name=source_name)
            names.append(job.get_item_name(self._items(job)[0]))
        self.assertEqual(len(set(names)), 3)

    def test_item_name_stays_within_255_chars_and_keeps_message_id(self):
        self._guild_with_general(messages=[_message(1)])
        job = self._make_job(name="x" * 400)
        name = job.get_item_name(self._items(job)[0])
        self.assertLessEqual(len(name), 255)
        self.assertTrue(name.endswith(_msg_id(1)))

    def test_process_item_stores_document_with_discord_metadata(self):
        self._guild_with_general(messages=[_message(1, "Hello world")])
        job = self._make_job()
        job.vector_manager.insert_documents = Mock()
        item = self._items(job)[0]
        with (
            patch.object(job.metadata_tracker, "get_latest_record", return_value=None),
            patch.object(job.metadata_tracker, "record_metadata") as record,
            patch.object(job.metadata_tracker, "delete_previous_embeddings"),
        ):
            self.assertEqual(job.process_item(item), 1)
        document = job.vector_manager.insert_documents.call_args[0][0][0]
        self.assertEqual(document.text, "Alice A. in #general:\nHello world")
        self.assertEqual(document.metadata["source"], "discord")
        self.assertEqual(document.metadata["key"], job.get_item_name(item))
        self.assertEqual(document.metadata["guild_id"], GUILD)
        self.assertEqual(document.metadata["url"], f"https://discord.com/channels/{GUILD}/{GENERAL}/{_msg_id(1)}")
        self.assertEqual(record.call_args[0][0], job.get_item_name(item))


def _response(status=200, body=None, headers=None):
    resp = MagicMock()
    resp.status_code = status
    resp.headers = headers or {}
    if body is None:
        resp.json.side_effect = ValueError("no body")
    else:
        resp.json.return_value = body
    return resp


class TestDiscordClient(unittest.TestCase):
    def setUp(self):
        session_patcher = patch("tasks.discord_ingestion.requests.Session")
        sleep_patcher = patch("tasks.discord_ingestion.time.sleep")
        self.mock_session_class = session_patcher.start()
        self.mock_sleep = sleep_patcher.start()
        self.addCleanup(session_patcher.stop)
        self.addCleanup(sleep_patcher.stop)
        self.session = self.mock_session_class.return_value
        self.client = DiscordClient("secret-token", max_retries=3)

    def test_sends_bot_authorization_header(self):
        self.assertEqual(self.session.headers.update.call_args[0][0]["Authorization"], "Bot secret-token")

    def test_close_closes_the_session(self):
        self.client.close()
        self.session.close.assert_called_once_with()

    def test_returns_json_on_success(self):
        self.session.get.return_value = _response(200, [{"id": "1"}])
        self.assertEqual(self.client.get_json("/channels/1/messages", params={"limit": 100}), [{"id": "1"}])
        self.session.get.assert_called_once_with(
            "https://discord.com/api/v10/channels/1/messages", params={"limit": 100}, timeout=30
        )
        self.mock_sleep.assert_not_called()

    def test_429_waits_fractional_json_retry_after(self):
        self.session.get.side_effect = [
            _response(429, {"retry_after": 0.35, "global": False}, {"Retry-After": "1"}),
            _response(200, {"ok": True}),
        ]
        self.assertEqual(self.client.get_json("/x"), {"ok": True})
        self.mock_sleep.assert_called_once_with(0.35)

    def test_429_falls_back_to_fractional_header(self):
        self.session.get.side_effect = [_response(429, None, {"Retry-After": "1.5"}), _response(200, {})]
        self.client.get_json("/x")
        self.mock_sleep.assert_called_once_with(1.5)

    def test_429_falls_back_to_exponential_backoff(self):
        self.session.get.side_effect = [_response(429, {}), _response(429, {}), _response(200, {})]
        self.client.get_json("/x")
        self.assertEqual([c.args[0] for c in self.mock_sleep.call_args_list], [1.0, 2.0])

    def test_global_429_is_logged_as_global_and_waited_out(self):
        self.session.get.side_effect = [_response(429, {"retry_after": 2.0, "global": True}), _response(200, {})]
        with self.assertLogs("tasks.discord_ingestion", level="WARNING") as logs:
            self.client.get_json("/x")
        self.mock_sleep.assert_called_once_with(2.0)
        self.assertIn("global rate limit", logs.output[0])

    def test_global_scope_header_is_detected_without_body(self):
        self.session.get.side_effect = [
            _response(429, None, {"Retry-After": "3", "X-RateLimit-Scope": "global"}),
            _response(200, {}),
        ]
        with self.assertLogs("tasks.discord_ingestion", level="WARNING") as logs:
            self.client.get_json("/x")
        self.mock_sleep.assert_called_once_with(3.0)
        self.assertIn("global rate limit", logs.output[0])

    def test_route_429_is_logged_as_route(self):
        self.session.get.side_effect = [_response(429, {"retry_after": 0.5, "global": False}), _response(200, {})]
        with self.assertLogs("tasks.discord_ingestion", level="WARNING") as logs:
            self.client.get_json("/x")
        self.assertIn("route rate limit", logs.output[0])

    def test_429_exhaustion_raises_after_bounded_attempts(self):
        self.session.get.return_value = _response(429, {"retry_after": 0.1})
        with self.assertRaises(DiscordAPIError) as ctx:
            self.client.get_json("/x")
        self.assertEqual(ctx.exception.status, 429)
        self.assertEqual(self.session.get.call_count, 4)
        self.assertEqual(self.mock_sleep.call_count, 3)

    def test_wait_above_limit_fails_without_sleeping(self):
        self.session.get.return_value = _response(429, {"retry_after": 1337.0})
        with self.assertRaises(DiscordAPIError):
            self.client.get_json("/x")
        self.mock_sleep.assert_not_called()
        self.assertEqual(self.session.get.call_count, 1)

    def test_server_error_retries_then_succeeds(self):
        self.session.get.side_effect = [_response(502), _response(200, {"ok": 1})]
        self.assertEqual(self.client.get_json("/x"), {"ok": 1})
        self.mock_sleep.assert_called_once_with(1.0)

    def test_server_error_exhaustion_raises(self):
        self.session.get.return_value = _response(503)
        with self.assertRaises(DiscordAPIError) as ctx:
            self.client.get_json("/x")
        self.assertEqual(ctx.exception.status, 503)
        self.assertEqual(self.session.get.call_count, 4)

    def test_network_error_retries_then_raises(self):
        self.session.get.side_effect = requests.ConnectionError("boom")
        with self.assertRaises(DiscordAPIError):
            self.client.get_json("/x")
        self.assertEqual(self.session.get.call_count, 4)

    def test_network_error_then_success(self):
        self.session.get.side_effect = [requests.Timeout("slow"), _response(200, {"ok": 1})]
        self.assertEqual(self.client.get_json("/x"), {"ok": 1})

    def test_invalid_token_is_not_retried(self):
        self.session.get.return_value = _response(401, {"message": "401: Unauthorized", "code": 0})
        with self.assertRaises(DiscordAuthError):
            self.client.get_json("/x")
        self.assertEqual(self.session.get.call_count, 1)
        self.mock_sleep.assert_not_called()

    def test_forbidden_is_not_retried_and_keeps_status_and_code(self):
        self.session.get.return_value = _response(403, {"message": "Missing Access", "code": 50001})
        with self.assertRaises(DiscordAPIError) as ctx:
            self.client.get_json("/channels/9")
        self.assertEqual((ctx.exception.status, ctx.exception.code), (403, 50001))
        self.assertIn("Missing Access", str(ctx.exception))
        self.assertEqual(self.session.get.call_count, 1)

    def test_exhausted_bucket_waits_for_reset_after(self):
        headers = {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset-After": "0.75"}
        self.session.get.return_value = _response(200, {}, headers)
        self.client.get_json("/x")
        self.mock_sleep.assert_called_once_with(0.75)

    def test_remaining_requests_do_not_wait(self):
        headers = {"X-RateLimit-Remaining": "3", "X-RateLimit-Reset-After": "0.75"}
        self.session.get.return_value = _response(200, {}, headers)
        self.client.get_json("/x")
        self.mock_sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
