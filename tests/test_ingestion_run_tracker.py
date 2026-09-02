"""Unit tests for IngestionRunTracker.update_progress."""

from contextlib import contextmanager
from unittest.mock import Mock, patch

from tasks.helper_classes.ingestion_run_tracker import IngestionRunTracker


@contextmanager
def _session_ctx(session):
    yield session


def _tracker_with_run(status: str):
    run = Mock(status=status)
    run.items_ingested = 0
    run.items_skipped = 0
    session = Mock()
    session.query.return_value.filter.return_value.first.return_value = run
    # get_db_session must be patched as a factory returning a fresh context
    # manager per call; patching it with the context manager instance itself
    # breaks __call__, and reusing one instance breaks on the second entry
    factory = Mock(side_effect=lambda: _session_ctx(session))
    return run, factory


def test_update_progress_updates_running_row():
    run, factory = _tracker_with_run("running")
    with patch(
        "tasks.helper_classes.ingestion_run_tracker.get_db_session",
        factory,
    ):
        IngestionRunTracker().update_progress(7, 3, 4)

    assert run.items_ingested == 3
    assert run.items_skipped == 4


def test_update_progress_ignores_completed_row():
    """A late flush must not overwrite the final complete_run record."""
    run, factory = _tracker_with_run("success")
    with patch(
        "tasks.helper_classes.ingestion_run_tracker.get_db_session",
        factory,
    ):
        IngestionRunTracker().update_progress(7, 3, 4)

    assert run.items_ingested == 0
    assert run.items_skipped == 0


def test_update_progress_noop_without_run_id():
    factory = Mock()
    with patch(
        "tasks.helper_classes.ingestion_run_tracker.get_db_session",
        factory,
    ):
        IngestionRunTracker().update_progress(None, 3, 4)

    factory.assert_not_called()


def test_update_progress_rate_limited_to_once_per_second():
    """Immediate repeat calls within 1s are dropped, the next one lands."""
    run, factory = _tracker_with_run("running")
    tracker = IngestionRunTracker()
    clock = Mock(side_effect=[100.0, 100.4, 101.5])
    with patch(
        "tasks.helper_classes.ingestion_run_tracker.get_db_session",
        factory,
    ):
        with patch(
            "tasks.helper_classes.ingestion_run_tracker.monotonic",
            clock,
        ):
            tracker.update_progress(7, 1, 0)  # writes
            tracker.update_progress(7, 2, 0)  # 0.4s later -> dropped
            tracker.update_progress(7, 2, 1)  # 1.5s after write -> writes

    assert factory.call_count == 2
    assert run.items_ingested == 2
    assert run.items_skipped == 1
