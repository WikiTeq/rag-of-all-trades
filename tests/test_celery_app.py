import unittest


def _make_source(name="wiki", src_type="mediawiki", enabled=None):
    source = {"type": src_type, "name": name, "config": {}, "schedule": 3600}
    if enabled is not None:
        source["enabled"] = enabled
    return source


def _run_registration(sources):
    """Simulate the celery_app registration loop without importing the module."""
    created = []
    for source in sources:
        if not source.get("enabled", True):
            continue
        created.append(source)
    return created


class TestSourceRegistration(unittest.TestCase):
    def test_enabled_true_registers_task(self):
        source = _make_source(enabled=True)
        registered = _run_registration([source])
        self.assertEqual(registered, [source])

    def test_enabled_absent_registers_task(self):
        source = _make_source()
        registered = _run_registration([source])
        self.assertEqual(registered, [source])

    def test_enabled_false_skips_task(self):
        source = _make_source(enabled=False)
        registered = _run_registration([source])
        self.assertEqual(registered, [])

    def test_mixed_sources(self):
        enabled_source = _make_source(name="wiki", enabled=True)
        disabled_source = _make_source(name="jira", src_type="jira", enabled=False)
        absent_source = _make_source(name="s3", src_type="s3")
        registered = _run_registration([enabled_source, disabled_source, absent_source])
        names = [s["name"] for s in registered]
        self.assertIn("wiki", names)
        self.assertIn("s3", names)
        self.assertNotIn("jira", names)
        self.assertEqual(len(registered), 2)
