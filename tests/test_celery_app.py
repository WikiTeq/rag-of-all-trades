import sys
import types
import unittest
from unittest.mock import MagicMock, patch

import yaml

from utils.celery_scheduling import MIN_SINGLETON_LOCK_EXPIRY, singleton_lock_expiry_for_schedule
from utils.parse import parse_bool

# ---------------------------------------------------------------------------
# Bootstrap: import celery_app with fake utils.config/utils.db so no real
# .env / config.yaml is needed (celery_app.py reads settings.env.REDIS_URL and
# settings.SOURCES at import time; utils.db reads settings for its engine).
# Same technique as tests/test_wipe_ingested.py.
# ---------------------------------------------------------------------------
_fake_env = MagicMock()
_fake_env.REDIS_URL = "redis://localhost:6379/0"
_fake_settings = MagicMock()
_fake_settings.env = _fake_env
_fake_settings.SOURCES = []  # skip the source-registration loop entirely

_fake_config = types.ModuleType("utils.config")
_fake_config.settings = _fake_settings

_fake_db = types.ModuleType("utils.db")
_fake_db.engine = MagicMock()

with patch.dict(sys.modules, {"utils.config": _fake_config, "utils.db": _fake_db}):
    import celery_app as _celery_app_module  # noqa: E402


class TestParseBoolEnabled(unittest.TestCase):
    def test_bool_true(self):
        self.assertTrue(parse_bool(True, default=True))

    def test_bool_false(self):
        self.assertFalse(parse_bool(False, default=True))

    def test_none_uses_default_true(self):
        self.assertTrue(parse_bool(None, default=True))

    def test_string_false_variants(self):
        for val in ("false", "False", "FALSE", "0", "no", "off"):
            self.assertFalse(parse_bool(val, default=True), f"Expected False for {val!r}")

    def test_string_true_variants(self):
        for val in ("true", "True", "TRUE", "1", "yes", "on"):
            self.assertTrue(parse_bool(val, default=True), f"Expected True for {val!r}")

    def test_yaml_unquoted_false(self):
        source = yaml.safe_load("enabled: false")
        self.assertFalse(parse_bool(source.get("enabled"), default=True))

    def test_yaml_unquoted_true(self):
        source = yaml.safe_load("enabled: true")
        self.assertTrue(parse_bool(source.get("enabled"), default=True))

    def test_yaml_absent_uses_default(self):
        source = yaml.safe_load("name: wiki")
        self.assertTrue(parse_bool(source.get("enabled"), default=True))


class TestSourceRegistration(unittest.TestCase):
    def _parse_sources(self, yaml_str):
        sources = yaml.safe_load(yaml_str)["sources"]
        return [{"enabled": parse_bool(s.get("enabled"), default=True), "name": s["name"]} for s in sources]

    def test_enabled_true_registers(self):
        parsed = self._parse_sources("sources:\n  - name: wiki\n    enabled: true\n")
        self.assertTrue(parsed[0]["enabled"])

    def test_enabled_false_skips(self):
        parsed = self._parse_sources("sources:\n  - name: wiki\n    enabled: false\n")
        self.assertFalse(parsed[0]["enabled"])

    def test_enabled_absent_defaults_to_true(self):
        parsed = self._parse_sources("sources:\n  - name: wiki\n")
        self.assertTrue(parsed[0]["enabled"])

    def test_mixed_sources(self):
        yaml_str = "sources:\n  - name: wiki\n    enabled: true\n  - name: jira\n    enabled: false\n  - name: s3\n"
        parsed = self._parse_sources(yaml_str)
        enabled_names = [s["name"] for s in parsed if s["enabled"]]
        self.assertIn("wiki", enabled_names)
        self.assertIn("s3", enabled_names)
        self.assertNotIn("jira", enabled_names)


class TestSingletonLockExpiry(unittest.TestCase):
    """MAIT-387: a worker killed mid-task never releases its celery_singleton lock
    (on_success/on_failure never fire), and with no lock_expiry the lock never
    expires either — the task is silently skipped forever after that. The lock
    TTL must be bounded so a leaked lock self-heals by the next scheduled run.
    """

    def test_expiry_doubles_schedule_when_above_floor(self):
        # Doubled, not equal to the schedule: a healthy task taking longer than one
        # interval must not have its lock expire out from under it (which would let
        # Beat dispatch a second, overlapping run).
        self.assertEqual(singleton_lock_expiry_for_schedule(3600), 7200)

    def test_expiry_uses_floor_for_frequent_schedules(self):
        self.assertEqual(singleton_lock_expiry_for_schedule(60), MIN_SINGLETON_LOCK_EXPIRY)

    def test_expiry_at_floor_boundary(self):
        # Doubling a schedule already at the floor exceeds it, so the result is the
        # doubled value, not the floor itself.
        expiry = singleton_lock_expiry_for_schedule(MIN_SINGLETON_LOCK_EXPIRY)
        self.assertEqual(expiry, MIN_SINGLETON_LOCK_EXPIRY * 2)

    def test_expiry_is_never_none_or_unbounded(self):
        # This is the actual bug: celery_singleton's default lock_expiry is None,
        # which means Redis SET ... EX=None -> no TTL -> permanent lock leak.
        for schedule in (60, 300, 3600, 7200):
            expiry = singleton_lock_expiry_for_schedule(schedule)
            self.assertIsNotNone(expiry)
            self.assertGreater(expiry, 0)

    def test_expiry_accepts_string_schedule(self):
        # source_config["schedule"] is parsed from YAML/env and may arrive as a string.
        self.assertEqual(singleton_lock_expiry_for_schedule("3600"), 7200)


class TestCreateTaskForSourceLockExpiryWiring(unittest.TestCase):
    """Registration-level check that create_task_for_source actually passes
    lock_expiry through to the Celery task decorator, not just that the pure
    helper computes the right number (TestSingletonLockExpiry above).
    """

    def test_registered_task_has_expected_lock_expiry(self):
        source_config = {
            "type": "jira",
            "name": "jira_wiring_test",
            "schedule": 3600,
            "config": {},
        }
        _celery_app_module.create_task_for_source(source_config)
        task_name = "jira_ingest_jira_wiring_test"

        task = _celery_app_module.celery_app.tasks[task_name]
        self.assertEqual(task.lock_expiry, 7200)

    def test_registered_task_uses_floor_for_frequent_schedule(self):
        source_config = {
            "type": "jira",
            "name": "jira_wiring_floor_test",
            "schedule": 60,
            "config": {},
        }
        _celery_app_module.create_task_for_source(source_config)
        task_name = "jira_ingest_jira_wiring_floor_test"

        task = _celery_app_module.celery_app.tasks[task_name]
        self.assertEqual(task.lock_expiry, MIN_SINGLETON_LOCK_EXPIRY)
