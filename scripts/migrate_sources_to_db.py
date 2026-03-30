#!/usr/bin/env python
"""ONE-TIME migration: move connector instances from config.yaml sources: to the DB.

Run ONCE before deploying the new code:
    python scripts/migrate_sources_to_db.py

After this script runs successfully, the sources: block in config.yaml is no longer
used by the application. Connector configuration is managed via the connector_instances
DB table from that point on.

Idempotent: rows where (type, name) already exists are skipped, so re-running is safe
but unnecessary.

S3 multi-bucket sources are expanded — each bucket becomes its own ConnectorInstance row,
matching the expansion logic that was previously in settings.SOURCES.
"""

import sys
from pathlib import Path

from sqlalchemy.exc import IntegrityError

# Ensure project root is on sys.path when run directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yaml  # noqa: E402

from models.connector_instance import ConnectorInstance  # noqa: E402
from utils.db import get_db_session  # noqa: E402
from utils.encryption import encrypt_secret  # noqa: E402

# Config keys treated as secrets — stripped from config JSONB and stored encrypted.
_SECRET_KEYS = frozenset(
    {
        "access_key",
        "secret_key",
        "api_key",
        "token",
        "password",
        "private_token",
        "client_secret",
    }
)


def _parse_schedule(schedules: list, index: int) -> int:
    """Return schedule in seconds from the schedules list at index, defaulting to 3600."""
    if index >= len(schedules):
        return 3600
    try:
        value = int(schedules[index])
    except (ValueError, TypeError):
        return 3600
    return value if value > 0 else 3600


def _extract_secrets(config: dict) -> tuple[dict, dict]:
    """Split config into (safe_config, secrets) where secrets contains _SECRET_KEYS values."""
    secrets = {k: v for k, v in config.items() if k in _SECRET_KEYS}
    safe_config = {k: v for k, v in config.items() if k not in _SECRET_KEYS}
    return safe_config, secrets


def _load_yaml(path: Path) -> dict:
    """Load a YAML file with ${ENV_VAR} interpolation from os.environ."""
    with open(path) as f:
        raw = f.read()
    import os

    for key, value in os.environ.items():
        raw = raw.replace(f"${{{key}}}", value)
    return yaml.safe_load(raw)


def _expand_sources(raw_sources: list) -> list[dict]:
    """Expand YAML sources into flat connector instance dicts (mirrors old settings.SOURCES)."""
    instances = []

    for source in raw_sources:
        src_type = source.get("type")
        name = source.get("name", "unknown_source")
        config = source.get("config", {})

        buckets = config.get("buckets", [])
        if isinstance(buckets, str):
            buckets = [b.strip() for b in buckets.split(",") if b.strip()]

        schedules = config.get("schedules", [])
        if isinstance(schedules, str):
            schedules = [s.strip() for s in schedules.split(",") if s.strip()]

        safe_config, secrets = _extract_secrets(config)

        if buckets:
            for i, bucket in enumerate(buckets):
                instances.append(
                    {
                        "type": src_type,
                        "name": f"{name}_{bucket}",
                        "schedule": _parse_schedule(schedules, i),
                        "config": {**safe_config, "buckets": [bucket], "bucket_override": bucket},
                        "secrets": secrets,
                    }
                )
        else:
            instances.append(
                {
                    "type": src_type,
                    "name": name,
                    "schedule": _parse_schedule(schedules, 0),
                    "config": safe_config,
                    "secrets": secrets,
                }
            )

    return instances


def main() -> None:
    """Read sources: from config.yaml and insert them as ConnectorInstance rows."""
    yaml_path = Path(__file__).resolve().parent.parent / "config.yaml"
    if not yaml_path.exists():
        print(f"config.yaml not found at {yaml_path}. Nothing to migrate.")
        return

    data = _load_yaml(yaml_path)
    raw_sources = data.get("sources", [])
    if not raw_sources:
        print("No sources: block found in config.yaml. Nothing to migrate.")
        return

    instances = _expand_sources(raw_sources)
    inserted = 0
    skipped = 0

    with get_db_session() as db:
        for inst in instances:
            existing = db.query(ConnectorInstance).filter_by(type=inst["type"], name=inst["name"]).first()
            if existing:
                print(f"  SKIP  {inst['type']}/{inst['name']} (already exists, id={existing.id})")
                skipped += 1
                continue

            row = ConnectorInstance(
                type=inst["type"],
                name=inst["name"],
                schedule=inst["schedule"],
                enabled=True,
                config=inst["config"],
                secret=encrypt_secret(inst["secrets"]) if inst["secrets"] else None,
            )
            try:
                db.add(row)
                db.flush()
                print(f"  INSERT {inst['type']}/{inst['name']} (schedule={inst['schedule']}s)")
                inserted += 1
            except IntegrityError:
                db.rollback()
                print(f"  SKIP  {inst['type']}/{inst['name']} (concurrent insert, already exists)")
                skipped += 1

    print(f"\nDone: {inserted} inserted, {skipped} skipped.")


if __name__ == "__main__":
    main()
