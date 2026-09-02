import json
import logging
import re
import secrets
import time
from datetime import UTC, datetime
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
from sqlalchemy import inspect, text

from celery_app import celery_app
from models.ingestion_run import IngestionRun
from utils.config import settings
from utils.db import get_db_session

router = APIRouter(tags=["Dashboard"])
logger = logging.getLogger(__name__)
dashboard_security = HTTPBasic()
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))


def validate_sql_identifier(identifier: str) -> str:
    if not IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Invalid SQL identifier: {identifier}")
    return identifier


def quote_sql_identifier(identifier: str) -> str:
    return f'"{validate_sql_identifier(identifier)}"'


def resolve_vector_table_name():
    configured_table_name = settings.POSTGRES.get("table_name", "embeddings")
    vector_table_name = f"data_{configured_table_name}"

    with get_db_session() as db:
        inspector = inspect(db.bind)
        if inspector.has_table(vector_table_name, schema="public"):
            return vector_table_name

    raise ValueError(f"Could not find vector table '{vector_table_name}' in schema 'public'")


def format_bytes(size_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(size_bytes)
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{int(size_bytes)} B"


def get_running_celery_jobs() -> int:
    try:
        inspector = celery_app.control.inspect(timeout=1.0)
        active = inspector.active() if inspector else {}
        if not active:
            return 0
        return sum(len(tasks) for tasks in active.values() if tasks)
    except Exception:
        return 0


def format_duration_ms(duration_ms: int | None) -> str:
    if duration_ms is None:
        return "-"
    if duration_ms < 1000:
        return f"{duration_ms} ms"
    if duration_ms < 60_000:
        return f"{duration_ms / 1000:.2f} s"
    minutes = duration_ms // 60_000
    seconds = (duration_ms % 60_000) / 1000
    return f"{minutes}m {seconds:.1f}s"


def serialize_ingestion_run(run: IngestionRun) -> dict:
    duration_ms = run.duration_ms
    if duration_ms is None and run.started_at:
        if run.completed_at:
            duration_ms = max(0, int((run.completed_at - run.started_at).total_seconds() * 1000))
        elif run.status == "running":
            # In-flight row: report live elapsed so the row moves between pushes
            started_at = run.started_at
            if started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=UTC)
            duration_ms = max(0, int((datetime.now(UTC) - started_at).total_seconds() * 1000))

    return {
        "id": run.id,
        "connector_name": run.connector_name,
        "connector_type": run.connector_type,
        "status": run.status,
        "items_ingested": run.items_ingested,
        "items_skipped": run.items_skipped,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "duration_ms": duration_ms,
        "duration_human": format_duration_ms(duration_ms),
        "error_message": run.error_message,
    }


def get_recent_ingestion_runs(limit: int = 10) -> list[dict]:
    try:
        with get_db_session() as db:
            inspector = inspect(db.bind)
            if not (inspector.has_table("ingestion_runs", schema="public") or inspector.has_table("ingestion_runs")):
                return []

            runs = db.query(IngestionRun).order_by(IngestionRun.started_at.desc()).limit(limit).all()
            return [serialize_ingestion_run(run) for run in runs]
    except Exception:
        logger.exception("Failed to fetch ingestion run records")
        return []


def get_dashboard_stats():
    vector_table_name = resolve_vector_table_name()

    with get_db_session() as db:
        # Vector Items = distinct documents at their latest version. Embedding
        # rows are chunks (SentenceSplitter), so counting rows overstates the
        # count ~3-4x. Old-version chunks are deleted on re-ingest, so distinct
        # key_text values == documents live in the store. Exact count by design:
        # no planner statistic can provide a DISTINCT count.
        vector_items_count = (
            db.execute(
                text(
                    f"""
                SELECT COUNT(DISTINCT key_text)
                FROM public.{quote_sql_identifier(vector_table_name)}
                """
                )
            ).scalar_one()
            or 0
        )
        vector_db_size_bytes = (
            db.execute(
                text("SELECT pg_total_relation_size(to_regclass(:relation_name))"),
                {"relation_name": f"public.{vector_table_name}"},
            ).scalar_one()
            or 0
        )

    return {
        "vector_table": vector_table_name,
        "vector_items_count": int(vector_items_count),
        "vector_db_size_bytes": int(vector_db_size_bytes),
        "vector_db_size_human": format_bytes(int(vector_db_size_bytes)),
        "running_celery_jobs": get_running_celery_jobs(),
        "configured_connectors_count": len(settings.SOURCES),
        "configured_connectors": [
            {
                "name": source.get("name"),
                "type": source.get("type"),
                "schedule_seconds": source.get("schedule"),
            }
            for source in settings.SOURCES
        ],
        "recent_ingestion_runs": get_recent_ingestion_runs(),
        "updated_at": datetime.now(UTC).isoformat(),
    }


def verify_dashboard_auth(credentials: HTTPBasicCredentials = Depends(dashboard_security)):
    username = settings.env.DASHBOARD_USER
    password = settings.env.DASHBOARD_PASS
    if not username or not password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )

    correct_username = secrets.compare_digest(credentials.username, username)
    correct_password = secrets.compare_digest(credentials.password, password)

    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )

    return credentials.username


@router.get("/dashboard", include_in_schema=False)
def dashboard_page(request: Request, _: str = Depends(verify_dashboard_auth)):
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={"refresh_interval_seconds": 30},
        # Template ships inline CSS; a heuristically cached page hides redesigns
        headers={"Cache-Control": "no-store"},
    )


@router.get("/dashboard/stats", include_in_schema=False)
def dashboard_stats(_: str = Depends(verify_dashboard_auth)):
    try:
        return get_dashboard_stats()
    except Exception:
        logger.exception("Failed to fetch dashboard stats")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch dashboard stats",
        )


SSE_DB_POLL_SECONDS = 5
SSE_MAX_PUSH_INTERVAL_SECONDS = 30
SSE_HEARTBEAT_INTERVAL_SECONDS = 15


def get_stats_fingerprint() -> tuple:
    """Cheap change detector for the stats payload.

    Covers the inputs the UI actually reflects: new/updated documents bump
    metadata.max(id), ingestion runs bump ingestion_runs.max(id), in-flight
    run progress moves the running-row counters, and DB size tracks
    deleted/added chunks. Celery job count is intentionally excluded
    (expensive inspect); it refreshes on the periodic full push instead.
    """
    vector_table_name = resolve_vector_table_name()
    with get_db_session() as db:
        row = db.execute(
            text(
                f"""
            SELECT (SELECT COALESCE(MAX(id), 0) FROM public.metadata),
                   (SELECT COALESCE(MAX(id), 0) FROM public.ingestion_runs),
                   (SELECT COALESCE(MAX(items_ingested), 0) FROM public.ingestion_runs WHERE status = 'running'),
                   (SELECT COALESCE(MAX(items_skipped), 0) FROM public.ingestion_runs WHERE status = 'running'),
                   pg_total_relation_size(to_regclass(:relation_name))
            """
            ),
            {"relation_name": f"public.{vector_table_name}"},
        ).one()
    return tuple(row)


def sse_event(payload: dict) -> str:
    return f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"


def dashboard_stats_stream():
    """Yield SSE events: full stats on change, at most every 30s regardless.

    Runs in the threadpool (sync generator); each client disconnect closes the
    generator. Heartbeat comments keep intermediaries from idling the
    connection out.
    """
    # object() sentinel: never equals a real fingerprint, forces the first push
    last_fingerprint = object()
    last_push = 0.0
    last_activity = time.monotonic()

    while True:
        try:
            fingerprint = get_stats_fingerprint()
        except Exception:
            logger.exception("Dashboard stream fingerprint failed")
            fingerprint = None

        now = time.monotonic()
        due = now - last_push >= SSE_MAX_PUSH_INTERVAL_SECONDS

        if fingerprint != last_fingerprint or due:
            try:
                yield sse_event(get_dashboard_stats())
                last_fingerprint = fingerprint
                last_push = now
                last_activity = now
            except Exception:
                logger.exception("Dashboard stream push failed")
        elif now - last_activity >= SSE_HEARTBEAT_INTERVAL_SECONDS:
            yield ": ping\n\n"
            last_activity = now

        time.sleep(SSE_DB_POLL_SECONDS)


@router.get("/dashboard/stream", include_in_schema=False)
def dashboard_stream(_: str = Depends(verify_dashboard_auth)):
    """Server-Sent Events stream of dashboard stats (push on change)."""
    return StreamingResponse(
        dashboard_stats_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
