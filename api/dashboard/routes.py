import logging
import re
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
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


def format_duration_ms(duration_ms: Optional[int]) -> str:
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
    if duration_ms is None and run.started_at and run.completed_at:
        duration_ms = max(0, int((run.completed_at - run.started_at).total_seconds() * 1000))

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
    }


def get_recent_ingestion_runs(limit: int = 10) -> list[dict]:
    try:
        with get_db_session() as db:
            inspector = inspect(db.bind)
            if not (
                inspector.has_table("ingestion_runs", schema="public")
                or inspector.has_table("ingestion_runs")
            ):
                return []

            runs = (
                db.query(IngestionRun)
                .order_by(IngestionRun.started_at.desc())
                .limit(limit)
                .all()
            )
            return [serialize_ingestion_run(run) for run in runs]
    except Exception:
        logger.exception("Failed to fetch ingestion run records")
        return []


def get_dashboard_stats():
    vector_table_name = resolve_vector_table_name()
    schema_sql = quote_sql_identifier("public")
    table_sql = quote_sql_identifier(vector_table_name)

    with get_db_session() as db:
        vector_items_count = db.execute(
            text(f"SELECT COUNT(*) FROM {schema_sql}.{table_sql}")
        ).scalar_one()
        vector_db_size_bytes = db.execute(
            text("SELECT pg_total_relation_size(to_regclass(:relation_name))"),
            {"relation_name": f"public.{vector_table_name}"},
        ).scalar_one() or 0

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
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def verify_dashboard_auth(credentials: HTTPBasicCredentials = Depends(dashboard_security)):
    correct_username = secrets.compare_digest(credentials.username, settings.env.DASHBOARD_USER)
    correct_password = secrets.compare_digest(credentials.password, settings.env.DASHBOARD_PASS)

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
