import unittest

from utils.config import parse_bool


class TestParseBool(unittest.TestCase):
    def test_true_bool(self):
        self.assertTrue(parse_bool(True))

    def test_false_bool(self):
        self.assertFalse(parse_bool(False))

    def test_truthy_strings(self):
        for value in ("true", "True", "TRUE", "1", "yes", "YES", "on", "ON"):
            with self.subTest(value=value):
                self.assertTrue(parse_bool(value))

    def test_falsy_strings(self):
        for value in ("false", "False", "FALSE", "0", "no", "NO", "off", "OFF"):
            with self.subTest(value=value):
                self.assertFalse(parse_bool(value))

    def test_none_returns_default_false(self):
        self.assertFalse(parse_bool(None))

    def test_none_returns_custom_default(self):
        self.assertTrue(parse_bool(None, default=True))

    def test_int_truthy(self):
        self.assertTrue(parse_bool(1))

    def test_int_falsy(self):
        self.assertFalse(parse_bool(0))

    def test_unrecognised_string_raises(self):
        with self.assertRaises(ValueError):
            parse_bool("maybe")

    def test_whitespace_stripped(self):
        self.assertTrue(parse_bool("  true  "))
        self.assertFalse(parse_bool("  false  "))


if __name__ == "__main__":
    unittest.main()
