"""
Script to manually trigger Celery ingestion tasks without waiting for beat schedules.

Useful on first deploy when schedules are set to long intervals (e.g. 3600s+).

Usage:
    docker compose exec celery_worker python scripts/trigger_ingestion.py
    docker compose exec celery_worker python scripts/trigger_ingestion.py --source jira1
    docker compose exec celery_worker python scripts/trigger_ingestion.py --dry-run
    docker compose exec celery_worker python scripts/trigger_ingestion.py --source jira1 --dry-run
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from celery_app import celery_app
from utils.celery_utils import ingestion_task_name
from utils.config import settings


def get_tasks(source_filter: str | None) -> list[dict]:
    """Return list of task dicts matching the optional source filter."""
    tasks = []
    for source in settings.SOURCES:
        if source_filter and source["name"] != source_filter:
            continue
        task_name = ingestion_task_name(source)
        if task_name not in celery_app.conf.beat_schedule:
            continue
        tasks.append(
            {
                "task_name": task_name,
                "source_name": source["name"],
                "source_type": source["type"],
            }
        )
    return tasks


def main() -> None:
    parser = argparse.ArgumentParser(description="Manually trigger Celery ingestion tasks registered in config.yaml.")
    parser.add_argument(
        "--source",
        help="Trigger only the source matching this name (as defined in config.yaml)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List tasks that would be triggered without actually enqueueing them",
    )
    args = parser.parse_args()

    tasks = get_tasks(args.source)

    if not tasks:
        if args.source:
            print(f"No registered tasks found for source: {args.source!r}")
        else:
            print("No registered tasks found.")
        sys.exit(1)

    if args.dry_run:
        print("Dry run — tasks that would be triggered:")
        for t in tasks:
            print(f"  {t['task_name']}  (source: {t['source_name']}, type: {t['source_type']})")
        return

    failed = []
    for t in tasks:
        try:
            celery_app.send_task(t["task_name"])
            print(f"Triggered: {t['task_name']}  (source: {t['source_name']}, type: {t['source_type']})")
        except Exception as e:
            print(f"FAILED: {t['task_name']}  (source: {t['source_name']}, type: {t['source_type']}): {e}")
            failed.append(t)

    print(f"\n{len(tasks) - len(failed)}/{len(tasks)} task(s) enqueued.")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
