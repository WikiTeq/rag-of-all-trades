import logging
from datetime import datetime, timezone
from typing import Optional

from models.ingestion_run import IngestionRun
from utils.db import get_db_session

logger = logging.getLogger(__name__)


class IngestionRunTracker:
    """Persists ingestion run lifecycle events for dashboard visibility."""

    def create_run(
        self,
        connector_name: str,
        connector_type: str,
        started_at: Optional[datetime] = None,
    ) -> Optional[int]:
        start_time = started_at or datetime.now(timezone.utc)
        try:
            with get_db_session() as db:
                run = IngestionRun(
                    connector_name=connector_name,
                    connector_type=connector_type,
                    status="running",
                    items_ingested=0,
                    items_skipped=0,
                    started_at=start_time,
                )
                db.add(run)
                db.flush()
                return run.id
        except Exception:
            logger.exception("Failed to create ingestion run record")
            return None

    def complete_run(
        self,
        run_id: Optional[int],
        status: str,
        items_ingested: int,
        items_skipped: int,
        completed_at: Optional[datetime] = None,
        duration_ms: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> None:
        if run_id is None:
            return

        end_time = completed_at or datetime.now(timezone.utc)
        try:
            with get_db_session() as db:
                run = (
                    db.query(IngestionRun)
                    .filter(IngestionRun.id == run_id)
                    .first()
                )
                if not run:
                    return

                started_at = run.started_at
                if started_at and started_at.tzinfo is None:
                    started_at = started_at.replace(tzinfo=timezone.utc)

                resolved_duration_ms = duration_ms
                if resolved_duration_ms is None and started_at:
                    resolved_duration_ms = max(
                        0,
                        int((end_time - started_at).total_seconds() * 1000),
                    )

                run.status = status
                run.items_ingested = items_ingested
                run.items_skipped = items_skipped
                run.completed_at = end_time
                run.duration_ms = resolved_duration_ms
                run.error_message = error_message
        except Exception:
            logger.exception("Failed to complete ingestion run record")
