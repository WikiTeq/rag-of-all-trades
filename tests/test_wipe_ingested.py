"""Tests for scripts/wipe_ingested.py."""

import sys
import types
import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Bootstrap: import the script with a fake utils.db so no real DB is needed.
# We patch sys.modules only for this import, then target wipe.__globals__
# for per-test patching (avoids the two-module problem that arises when
# `import scripts.wipe_ingested` and `from ... import wipe` see different
# module objects in sys.modules after patch.dict restores utils entries).
# ---------------------------------------------------------------------------
_fake_utils = types.ModuleType("utils")
_fake_db = types.ModuleType("utils.db")


@contextmanager
def _stub_get_db_session():
    yield MagicMock()


_fake_db.get_db_session = _stub_get_db_session
_fake_utils.db = _fake_db

with patch.dict(sys.modules, {"utils": _fake_utils, "utils.db": _fake_db}):
    from scripts.wipe_ingested import parse_filter, wipe  # noqa: E402

# The module wipe actually lives in (may differ from sys.modules entry after
# patch.dict exits, so we derive it from the function itself).
_WIPE_GLOBALS = wipe.__globals__


@contextmanager
def _session_ctx(session):
    yield session


def _make_db_patch(session: MagicMock):
    """Return a patch context manager that makes wipe() use the given session."""
    return patch.dict(_WIPE_GLOBALS, {"get_db_session": lambda: _session_ctx(session)})


def _mock_session(rowcounts: list[int]) -> MagicMock:
    session = MagicMock()
    results = []
    for n in rowcounts:
        r = MagicMock()
        r.rowcount = n
        results.append(r)
    session.execute.side_effect = results
    return session


class TestParseFilter(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(parse_filter("entity_type=note"), ("entity_type", "note"))

    def test_value_with_equals(self):
        self.assertEqual(parse_filter("key=a=b"), ("key", "a=b"))

    def test_missing_equals(self):
        import argparse

        with self.assertRaises(argparse.ArgumentTypeError):
            parse_filter("noequalssign")

    def test_empty_key(self):
        import argparse

        with self.assertRaises(argparse.ArgumentTypeError):
            parse_filter("=value")

    def test_empty_value(self):
        import argparse

        with self.assertRaises(argparse.ArgumentTypeError):
            parse_filter("key=")


class TestWipe(unittest.TestCase):
    def test_full_wipe_issues_two_server_side_deletes(self):
        session = _mock_session([2, 2])
        with _make_db_patch(session):
            wipe(None, None, None)
        self.assertEqual(session.execute.call_count, 2)
        sqls = [str(c.args[0]) for c in session.execute.call_args_list]
        self.assertTrue(any("data_embeddings" in s for s in sqls))
        self.assertTrue(any("FROM metadata" in s for s in sqls))

    def test_source_wipe_filters_by_source_name(self):
        session = _mock_session([2, 2])
        with _make_db_patch(session):
            wipe("pipedrive1", None, None)
        self.assertEqual(session.execute.call_count, 2)
        sqls = [str(c.args[0]) for c in session.execute.call_args_list]
        self.assertTrue(all("source_name" in s for s in sqls))

    def test_source_and_filter_wipe(self):
        session = _mock_session([1, 1])
        with _make_db_patch(session):
            wipe("pipedrive1", "entity_type", "note")
        self.assertEqual(session.execute.call_count, 2)
        sqls = [str(c.args[0]) for c in session.execute.call_args_list]
        self.assertTrue(any(":fk" in s for s in sqls))

    def test_no_matching_records_prints_message(self):
        session = _mock_session([0, 0])
        with _make_db_patch(session), patch("builtins.print") as mock_print:
            wipe("pipedrive1", None, None)
        self.assertEqual(session.execute.call_count, 2)
        mock_print.assert_called_once_with("No records found for source_name='pipedrive1'.")

    def test_filter_requires_source(self):
        with self.assertRaises(ValueError):
            wipe(None, "entity_type", "note")

    def test_no_python_side_any_keys(self):
        # Deletes must be server-side subqueries, not Python-collected key lists
        session = _mock_session([1, 1])
        with _make_db_patch(session):
            wipe("pipedrive1", None, None)
        sqls = [str(c.args[0]) for c in session.execute.call_args_list]
        self.assertFalse(any("ANY(:keys)" in s for s in sqls))


class TestMain(unittest.TestCase):
    def _main(self, argv):
        with patch.dict(_WIPE_GLOBALS, {"get_db_session": _stub_get_db_session}):
            with patch("sys.argv", ["wipe_ingested.py"] + argv):
                from scripts.wipe_ingested import main

                return main()

    def test_no_args_prints_help_and_exits(self):
        with patch("sys.argv", ["wipe_ingested.py"]):
            from scripts.wipe_ingested import main

            with self.assertRaises(SystemExit) as cm:
                main()
        self.assertEqual(cm.exception.code, 1)

    def test_all_and_source_mutually_exclusive(self):
        with patch("sys.argv", ["wipe_ingested.py", "--all", "--source", "pipedrive1"]):
            from scripts.wipe_ingested import main

            with self.assertRaises(SystemExit):
                main()

    def test_filter_without_source_errors(self):
        with patch("sys.argv", ["wipe_ingested.py", "--all", "--filter", "entity_type=note"]):
            from scripts.wipe_ingested import main

            with self.assertRaises(SystemExit):
                main()


if __name__ == "__main__":
    unittest.main()
