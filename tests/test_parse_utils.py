import unittest
from datetime import UTC, datetime

from utils.parse import parse_bool, parse_list, parse_timestamp


class TestParseTimestamp(unittest.TestCase):
    def test_valid_iso_string(self):
        result = parse_timestamp("2024-01-15T10:30:00.000+0000")
        self.assertIsInstance(result, datetime)
        self.assertEqual(result.year, 2024)

    def test_z_suffix(self):
        result = parse_timestamp("2024-06-01T00:00:00Z")
        self.assertIsInstance(result, datetime)
        self.assertEqual(result.tzinfo, UTC)

    def test_datetime_passthrough(self):
        dt = datetime(2024, 1, 1, tzinfo=UTC)
        self.assertIs(parse_timestamp(dt), dt)

    def test_none_returns_none(self):
        self.assertIsNone(parse_timestamp(None))

    def test_malformed_string_returns_none(self):
        self.assertIsNone(parse_timestamp("not-a-date"))

    def test_non_string_non_datetime_returns_none(self):
        self.assertIsNone(parse_timestamp(12345))


class TestParseList(unittest.TestCase):
    def test_comma_string(self):
        self.assertEqual(parse_list("a, b, c"), ["a", "b", "c"])

    def test_native_list(self):
        self.assertEqual(parse_list(["x", "y"]), ["x", "y"])

    def test_none_returns_empty(self):
        self.assertEqual(parse_list(None), [])

    def test_filters_empty_items(self):
        self.assertEqual(parse_list("a,,b, "), ["a", "b"])

    def test_lower(self):
        self.assertEqual(parse_list("MD,TXT", lower=True), ["md", "txt"])

    def test_set_input(self):
        result = parse_list({"py", "md"})
        self.assertEqual(sorted(result), ["md", "py"])

    def test_single_item_string(self):
        self.assertEqual(parse_list("only"), ["only"])


class TestParseBool(unittest.TestCase):
    def test_none_returns_default_false(self):
        self.assertFalse(parse_bool(None))

    def test_none_returns_default_true(self):
        self.assertTrue(parse_bool(None, default=True))

    def test_bool_true_passthrough(self):
        self.assertTrue(parse_bool(True))

    def test_bool_false_passthrough(self):
        self.assertFalse(parse_bool(False))

    def test_truthy_strings(self):
        for s in ("true", "True", "TRUE", "yes", "YES", "1", "on", "ON"):
            with self.subTest(s=s):
                self.assertTrue(parse_bool(s))

    def test_falsy_strings(self):
        for s in ("false", "False", "no", "0", "off", "random"):
            with self.subTest(s=s):
                self.assertFalse(parse_bool(s))


if __name__ == "__main__":
    unittest.main()
