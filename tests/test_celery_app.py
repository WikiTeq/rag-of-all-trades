import unittest

import yaml

from utils.parse import parse_bool


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
