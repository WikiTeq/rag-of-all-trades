"""
Maintenance script for selective wiping of ingested records.

Deletes rows from both `metadata` and `data_embeddings` tables.

Usage:
    docker compose exec api python scripts/wipe_ingested.py --all
    docker compose exec api python scripts/wipe_ingested.py --source pipedrive1
    docker compose exec api python scripts/wipe_ingested.py --source pipedrive1 --filter entity_type=note

The --filter flag accepts a single key=value pair matched against the
`metadata_` JSONB column in data_embeddings. Requires --source.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from utils.db import get_db_session


def wipe(source: str | None, filter_key: str | None, filter_value: str | None) -> None:
    if filter_key is not None and source is None:
        raise ValueError("filter_key requires source")

    with get_db_session() as db:
        if source is None and filter_key is None:
            # Full wipe — server-side, no Python-side key collection
            del_emb = db.execute(text("DELETE FROM public.data_embeddings"))
            del_meta = db.execute(text("DELETE FROM metadata"))
        elif filter_key is None:
            # Wipe by source — server-side join on computed key_text
            del_emb = db.execute(
                text(
                    "DELETE FROM public.data_embeddings "
                    "WHERE key_text IN ("
                    "  SELECT key FROM metadata "
                    "  WHERE metadata_content->>'source_name' = :source"
                    ")"
                ),
                {"source": source},
            )
            del_meta = db.execute(
                text("DELETE FROM metadata WHERE metadata_content->>'source_name' = :source"),
                {"source": source},
            )
        else:
            # Wipe by source + metadata filter — filter on data_embeddings JSONB
            del_emb = db.execute(
                text(
                    "DELETE FROM public.data_embeddings "
                    "WHERE key_text IN ("
                    "  SELECT key FROM metadata "
                    "  WHERE metadata_content->>'source_name' = :source"
                    ") "
                    "AND metadata_->>:fk = :fv"
                ),
                {"source": source, "fk": filter_key, "fv": filter_value},
            )
            # Only delete metadata rows for this source+filter that have no remaining embeddings
            del_meta = db.execute(
                text(
                    "DELETE FROM metadata "
                    "WHERE metadata_content->>'source_name' = :source "
                    "AND key NOT IN ("
                    "  SELECT key_text FROM public.data_embeddings "
                    "  WHERE key_text IN ("
                    "    SELECT key FROM metadata "
                    "    WHERE metadata_content->>'source_name' = :source"
                    "  ) "
                    "  AND metadata_->>:fk = :fv"
                    ")"
                ),
                {"source": source, "fk": filter_key, "fv": filter_value},
            )

        if del_emb.rowcount == 0 and del_meta.rowcount == 0:
            if source:
                msg = f"source_name={source!r}"
                if filter_key:
                    msg += f", {filter_key}={filter_value!r}"
                print(f"No records found for {msg}.")
            else:
                print("No records found.")
            return

        print(f"Deleted {del_emb.rowcount} row(s) from data_embeddings.")
        print(f"Deleted {del_meta.rowcount} row(s) from metadata.")


def parse_filter(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError(f"--filter must be in key=value format, got: {value!r}")
    key, _, val = value.partition("=")
    if not key or not val:
        raise argparse.ArgumentTypeError(f"--filter key and value must be non-empty, got: {value!r}")
    return key, val


def main() -> None:
    parser = argparse.ArgumentParser(description="Selectively wipe ingested records from the database.")
    parser.add_argument("--all", action="store_true", help="Wipe all ingested records")
    parser.add_argument("--source", help="Connector source_name (e.g. pipedrive1)")
    parser.add_argument(
        "--filter",
        dest="filter",
        metavar="KEY=VALUE",
        help="Extra metadata filter (e.g. entity_type=note). Requires --source.",
    )
    args = parser.parse_args()

    if not args.all and not args.source:
        parser.print_help()
        sys.exit(1)

    if args.all and args.source:
        parser.error("--all and --source are mutually exclusive")

    filter_key: str | None = None
    filter_value: str | None = None

    if args.filter:
        if not args.source:
            parser.error("--filter requires --source")
        try:
            filter_key, filter_value = parse_filter(args.filter)
        except argparse.ArgumentTypeError as exc:
            parser.error(str(exc))

    wipe(None if args.all else args.source, filter_key, filter_value)


if __name__ == "__main__":
    sys.exit(main())
