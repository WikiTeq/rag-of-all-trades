import logging
import unittest

from utils.cache import CachedResolver


class TestCachedResolver(unittest.TestCase):
    def test_calls_fetch_fn_once(self):
        calls = []

        def fetch(key):
            calls.append(key)
            return f"value:{key}"

        resolver = CachedResolver(fetch)
        self.assertEqual(resolver.resolve("a"), "value:a")
        self.assertEqual(resolver.resolve("a"), "value:a")
        self.assertEqual(len(calls), 1)

    def test_different_keys_each_fetched_once(self):
        resolver = CachedResolver(lambda k: k.upper())
        self.assertEqual(resolver.resolve("x"), "X")
        self.assertEqual(resolver.resolve("y"), "Y")

    def test_none_result_is_cached(self):
        calls = []

        def fetch(key):
            calls.append(key)
            return None

        resolver = CachedResolver(fetch)
        resolver.resolve("k")
        resolver.resolve("k")
        self.assertEqual(len(calls), 1)

    def test_fetch_exception_returns_none_and_not_cached(self):
        attempts = []

        def fetch(key):
            attempts.append(key)
            raise ValueError("api down")

        resolver = CachedResolver(fetch, logging.getLogger("test"))
        result1 = resolver.resolve("bad")
        result2 = resolver.resolve("bad")
        self.assertIsNone(result1)
        self.assertIsNone(result2)
        # Not cached on failure — each call retries
        self.assertEqual(len(attempts), 2)

    def test_clear_resets_cache(self):
        calls = []

        def fetch(key):
            calls.append(key)
            return "v"

        resolver = CachedResolver(fetch)
        resolver.resolve("k")
        resolver.clear()
        resolver.resolve("k")
        self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()
