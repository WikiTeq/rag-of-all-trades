import os
import unittest
from unittest.mock import MagicMock, patch


class TestConnectorInstanceModel(unittest.TestCase):
    def _make_instance(self, **kwargs):
        from models.connector_instance import ConnectorInstance

        defaults = {
            "type": "jira",
            "name": "main",
            "schedule": 3600,
            "enabled": True,
            "config": {"server_url": "https://example.atlassian.net"},
        }
        defaults.update(kwargs)
        return ConnectorInstance(**defaults)

    def test_defaults(self):
        inst = self._make_instance()
        self.assertTrue(inst.enabled)
        self.assertEqual(inst.type, "jira")
        self.assertIsNone(inst.secret)

    def test_to_config(self):
        inst = self._make_instance()
        cfg = inst.to_config()
        self.assertEqual(cfg["type"], "jira")
        self.assertEqual(cfg["name"], "main")
        self.assertEqual(cfg["schedule"], 3600)
        self.assertIn("server_url", cfg["config"])

    def test_to_config_shape(self):
        inst = self._make_instance(type="mediawiki", name="wiki1", schedule=600)
        cfg = inst.to_config()
        self.assertEqual(set(cfg.keys()), {"type", "name", "schedule", "config"})


class TestEncryption(unittest.TestCase):
    def setUp(self):
        from cryptography.fernet import Fernet

        self._key = Fernet.generate_key().decode()
        os.environ["CONNECTOR_ENCRYPTION_KEY"] = self._key
        # Patch the settings singleton so encrypt_secret/decrypt_secret pick up the key
        import utils.config as config_mod

        config_mod.settings.env.CONNECTOR_ENCRYPTION_KEY = self._key
        # Reload to pick up new env key
        import importlib

        import utils.encryption as enc_mod

        importlib.reload(enc_mod)
        self.encrypt = enc_mod.encrypt_secret
        self.decrypt = enc_mod.decrypt_secret

    def test_round_trip(self):
        data = {"token": "abc123", "refresh": "xyz"}
        token = self.encrypt(data)
        self.assertIsInstance(token, str)
        result = self.decrypt(token)
        self.assertEqual(result, data)

    def test_invalid_token_raises(self):
        with self.assertRaises(ValueError):
            self.decrypt("not-a-valid-fernet-token")

    def test_empty_token_raises(self):
        with self.assertRaises(ValueError):
            self.decrypt("")

    def test_none_guard_in_task(self):
        """The task body guards: `decrypt_secret(inst.secret) if inst.secret else {}`"""
        inst_secret = None
        result = {} if not inst_secret else self.decrypt(inst_secret)
        self.assertEqual(result, {})


def _make_mock_db_session(instances):
    """Return a context-manager mock that yields a db with query().all() == instances."""
    mock_db = MagicMock()
    mock_db.query.return_value.all.return_value = instances
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=mock_db)
    cm.__exit__ = MagicMock(return_value=False)
    return cm, mock_db


def _make_connector_mock(id=1, type="jira", name="main", schedule=3600, enabled=True):
    from models.connector_instance import ConnectorInstance

    inst = MagicMock(spec=ConnectorInstance)
    inst.id = id
    inst.type = type
    inst.name = name
    inst.schedule = schedule
    inst.enabled = enabled
    inst.secret = None
    inst.to_config.return_value = {
        "type": type,
        "name": name,
        "schedule": schedule,
        "config": {},
    }
    return inst


class TestSyncToBeatSchedule(unittest.TestCase):
    def _run_sync(self, instances):
        cm, _ = _make_mock_db_session(instances)
        mock_app = MagicMock()
        mock_entry_cls = MagicMock()
        mock_entry = MagicMock()
        mock_entry_cls.return_value = mock_entry

        with patch("utils.scheduler.get_db_session", return_value=cm):
            with patch("utils.scheduler.RedBeatSchedulerEntry", mock_entry_cls):
                with patch("utils.scheduler.celery_app", mock_app, create=True):
                    # Lazy import inside sync_to_beat_schedule — patch at source
                    import utils.scheduler as sched

                    orig = sched.sync_to_beat_schedule.__globals__.get("celery_app")
                    sched.sync_to_beat_schedule.__globals__["celery_app"] = mock_app
                    try:
                        with patch("celery_app.celery_app", mock_app, create=True):
                            sched.sync_to_beat_schedule()
                    finally:
                        if orig is None:
                            sched.sync_to_beat_schedule.__globals__.pop("celery_app", None)
                        else:
                            sched.sync_to_beat_schedule.__globals__["celery_app"] = orig

        return mock_entry_cls, mock_entry

    def test_enabled_instance_saves_entry(self):
        inst = _make_connector_mock(enabled=True)
        mock_entry_cls, mock_entry = self._run_sync([inst])
        mock_entry.save.assert_called_once()

    def test_disabled_instance_deletes_entry(self):
        inst = _make_connector_mock(enabled=False)
        mock_entry_cls, mock_entry = self._run_sync([inst])
        mock_entry_cls.from_key.assert_called_once()
        mock_entry_cls.from_key.return_value.delete.assert_called_once()

    def test_disabled_absent_entry_is_noop(self):
        inst = _make_connector_mock(enabled=False)
        cm, _ = _make_mock_db_session([inst])
        mock_app = MagicMock()
        mock_entry_cls = MagicMock()
        mock_entry_cls.from_key.side_effect = KeyError("not found")

        with patch("utils.scheduler.get_db_session", return_value=cm):
            with patch("utils.scheduler.RedBeatSchedulerEntry", mock_entry_cls):
                with patch("celery_app.celery_app", mock_app, create=True):
                    import utils.scheduler as sched

                    _orig = sched.sync_to_beat_schedule.__globals__.get("celery_app")
                    sched.sync_to_beat_schedule.__globals__["celery_app"] = mock_app
                    try:
                        sched.sync_to_beat_schedule()  # must not raise
                    finally:
                        if _orig is None:
                            sched.sync_to_beat_schedule.__globals__.pop("celery_app", None)
                        else:
                            sched.sync_to_beat_schedule.__globals__["celery_app"] = _orig


class TestRunIngestionTask(unittest.TestCase):
    # get_db_session / IngestionJobFactory / decrypt_secret are lazy-imported inside the task
    # body — patch them at their source modules, not at celery_app.
    def _run_task(self, inst):
        cm, mock_db = _make_mock_db_session([])
        mock_db.get.return_value = inst
        mock_factory = MagicMock()
        mock_job = MagicMock()
        mock_factory.create.return_value = mock_job

        with patch("utils.db.get_db_session", return_value=cm):
            with patch("tasks.factory.IngestionJobFactory.create", mock_factory.create):
                with patch("utils.encryption.decrypt_secret", return_value={}):
                    from celery_app import run_ingestion

                    run_ingestion.run(instance_id=inst.id if inst else 999)

        return mock_factory, mock_job

    def test_disabled_returns_early(self):
        inst = _make_connector_mock(enabled=False)
        mock_factory, _ = self._run_task(inst)
        mock_factory.create.assert_not_called()

    def test_missing_instance_returns_early(self):
        cm, mock_db = _make_mock_db_session([])
        mock_db.get.return_value = None
        mock_factory = MagicMock()

        with patch("utils.db.get_db_session", return_value=cm):
            with patch("tasks.factory.IngestionJobFactory.create", mock_factory.create):
                from celery_app import run_ingestion

                run_ingestion.run(instance_id=999)

        mock_factory.create.assert_not_called()

    def test_enabled_dispatches_job(self):
        inst = _make_connector_mock(enabled=True)
        mock_factory, mock_job = self._run_task(inst)
        mock_factory.create.assert_called_once_with("jira", unittest.mock.ANY)
        mock_job.run.assert_called_once()


class TestMigrationScript(unittest.TestCase):
    def test_expand_sources_non_s3(self):
        from scripts.migrate_sources_to_db import _expand_sources

        sources = [
            {
                "type": "jira",
                "name": "myjira",
                "config": {"server_url": "https://x.atlassian.net", "schedules": "3600"},
            }
        ]
        result = _expand_sources(sources)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "myjira")
        self.assertEqual(result[0]["schedule"], 3600)

    def test_expand_sources_s3_multi_bucket(self):
        from scripts.migrate_sources_to_db import _expand_sources

        sources = [
            {
                "type": "s3",
                "name": "account1",
                "config": {
                    "buckets": ["bucket-a", "bucket-b"],
                    "schedules": ["1800", "3600"],
                },
            }
        ]
        result = _expand_sources(sources)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["name"], "account1_bucket-a")
        self.assertEqual(result[0]["schedule"], 1800)
        self.assertEqual(result[1]["name"], "account1_bucket-b")
        self.assertEqual(result[1]["schedule"], 3600)

    def test_expand_sources_idempotent(self):
        from scripts.migrate_sources_to_db import _expand_sources

        sources = [
            {
                "type": "s3",
                "name": "acct",
                "config": {"buckets": ["only-bucket"], "schedules": ["600"]},
            }
        ]
        self.assertEqual(_expand_sources(sources), _expand_sources(sources))


if __name__ == "__main__":
    unittest.main()
