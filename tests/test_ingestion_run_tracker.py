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
    # get_db_session must be patched as a factory returning a context manager;
    # patching it with the context manager instance itself breaks __call__
    factory = Mock(return_value=_session_ctx(session))
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
