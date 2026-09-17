import sys
import types
import unittest
from unittest.mock import MagicMock, patch

import yaml

from utils.celery_scheduling import SINGLETON_LOCK_EXPIRY, SINGLETON_LOCK_RENEWAL_INTERVAL
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
    expires either — the task is silently skipped forever after that.

    The TTL is a fixed, short constant, independent of any task's schedule —
    a healthy task stays alive via HeartbeatingSingleton's periodic renewal
    (tests/test_celery_heartbeat_singleton.py), not by having a long enough TTL.
    This class only checks the constants are sane; the renewal behavior itself
    is covered by TestCreateTaskForSourceLockExpiryWiring below and the
    HeartbeatingSingleton test module.
    """

    def test_expiry_is_positive(self):
        self.assertGreater(SINGLETON_LOCK_EXPIRY, 0)

    def test_renewal_interval_is_positive(self):
        self.assertGreater(SINGLETON_LOCK_RENEWAL_INTERVAL, 0)

    def test_renewal_interval_leaves_headroom_before_expiry(self):
        # Renewal must land comfortably before the TTL, not right at the wire.
        self.assertLess(SINGLETON_LOCK_RENEWAL_INTERVAL, SINGLETON_LOCK_EXPIRY)


class TestCreateTaskForSourceLockExpiryWiring(unittest.TestCase):
    """Registration-level check that create_task_for_source wires the fixed
    lock_expiry and the HeartbeatingSingleton base into the Celery task
    decorator, not just that the constants themselves are sane.
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
        self.assertEqual(task.lock_expiry, SINGLETON_LOCK_EXPIRY)

    def test_registered_task_uses_heartbeating_singleton_base(self):
        source_config = {
            "type": "jira",
            "name": "jira_wiring_heartbeat_test",
            "schedule": 3600,
            "config": {},
        }
        _celery_app_module.create_task_for_source(source_config)
        task_name = "jira_ingest_jira_wiring_heartbeat_test"

        task = _celery_app_module.celery_app.tasks[task_name]
        self.assertIsInstance(task, _celery_app_module.HeartbeatingSingleton)
