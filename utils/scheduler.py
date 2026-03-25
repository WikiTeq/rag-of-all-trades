import logging

from celery.schedules import schedule as celery_schedule
from redbeat import RedBeatSchedulerEntry

from models.connector_instance import ConnectorInstance
from utils.db import get_db_session

logger = logging.getLogger(__name__)


def sync_to_beat_schedule() -> None:
    """Sync enabled/disabled ConnectorInstance rows to RedBeat scheduler entries.

    Idempotent — safe to call repeatedly (e.g. on Beat startup or after a DB write).
    Enabled instances get a RedBeat entry created or updated.
    Disabled instances have their RedBeat entry removed if present.

    Must only be called from the Beat process (via beat_init signal) to avoid
    concurrent write races from multiple worker processes.
    """
    from celery_app import celery_app  # avoid circular import at module level

    with get_db_session() as db:
        all_instances = db.query(ConnectorInstance).all()
        # Read all needed fields inside session to avoid DetachedInstanceError after close
        snapshot = [
            {
                "id": i.id,
                "type": i.type,
                "name": i.name,
                "schedule": i.schedule,
                "enabled": i.enabled,
            }
            for i in all_instances
        ]

    enabled = [i for i in snapshot if i["enabled"]]
    disabled = [i for i in snapshot if not i["enabled"]]

    for inst in enabled:
        entry_name = f"{inst['type']}_ingest_{inst['name']}"
        entry = RedBeatSchedulerEntry(
            entry_name,
            "run_ingestion",
            celery_schedule(run_every=inst["schedule"]),
            kwargs={"instance_id": inst["id"]},
            app=celery_app,
        )
        entry.save()

    for inst in disabled:
        entry_name = f"{inst['type']}_ingest_{inst['name']}"
        try:
            entry = RedBeatSchedulerEntry.from_key(f"redbeat:{entry_name}", app=celery_app)
            entry.delete()
            logger.info("Removed Beat entry for disabled connector: %s", entry_name)
        except KeyError:
            pass  # already absent — no-op

    logger.info(
        "sync_to_beat_schedule: %d enabled, %d disabled",
        len(enabled),
        len(disabled),
    )
