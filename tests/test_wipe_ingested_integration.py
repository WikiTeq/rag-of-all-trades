"""Real-database regression test for scripts/wipe_ingested.py (MAIT-349).

Connects to the local Postgres (same instance used by `docker compose up`,
reachable on localhost:5432 from the host) and exercises wipe() against real
rows, since the unit tests in test_wipe_ingested.py only assert query text
against a mocked session and cannot catch bugs in the query's runtime
behavior. Skips if that database isn't reachable.
"""

import os
import unittest
import uuid
from contextlib import contextmanager
from unittest.mock import patch

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

TEST_DATABASE_URL = os.environ.get(
    "WIPE_TEST_DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/rag",
)


def _db_available() -> bool:
    try:
        engine = create_engine(TEST_DATABASE_URL)
        with engine.connect():
            return True
    except Exception:
        return False
    finally:
        engine.dispose()


@unittest.skipUnless(_db_available(), f"Postgres not reachable at {TEST_DATABASE_URL}")
class TestWipeIngestedIntegration(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(TEST_DATABASE_URL)
        self.Session = sessionmaker(bind=self.engine)
        self.source = f"test_source_{uuid.uuid4().hex[:8]}"
        self.other_source = f"test_source_{uuid.uuid4().hex[:8]}"
        self._seed()

    def tearDown(self):
        with self.engine.begin() as conn:
            conn.execute(
                text("DELETE FROM public.data_embeddings WHERE metadata_->>'source_name' IN (:s, :o)"),
                {"s": self.source, "o": self.other_source},
            )
            conn.execute(
                text("DELETE FROM public.metadata WHERE metadata_content->>'source_name' IN (:s, :o)"),
                {"s": self.source, "o": self.other_source},
            )
        self.engine.dispose()

    def _seed(self):
        # metadata.metadata_content only ever carries source_name in production
        # (see tasks/base.py's BaseMetadataSchema + record_metadata call);
        # entity_type lives only in data_embeddings.metadata_, set via each
        # connector's get_extra_metadata(). Mirror that shape here so the test
        # reflects real ingested data, not a synthetic fixture.
        rows = [
            # (key, entity_type, source)
            (f"{self.source}:notes:1", "notes", self.source),
            (f"{self.source}:notes:2", "notes", self.source),
            (f"{self.source}:deals:1", "deals", self.source),
            (f"{self.other_source}:notes:1", "notes", self.other_source),
        ]
        with self.engine.begin() as conn:
            for key, entity_type, source in rows:
                checksum = f"chk-{key}"
                conn.execute(
                    text(
                        "INSERT INTO public.metadata (key, checksum, version, metadata_content, last_modified) "
                        "VALUES (:key, :checksum, 1, jsonb_build_object('source_name', :source), now())"
                    ),
                    {"key": key, "checksum": checksum, "source": source},
                )
                conn.execute(
                    text(
                        "INSERT INTO public.data_embeddings (text, metadata_) "
                        "VALUES (:text, jsonb_build_object("
                        "  'key', :key, 'checksum', :checksum, 'source_name', :source, 'entity_type', :entity_type"
                        "))"
                    ),
                    {"text": key, "key": key, "checksum": checksum, "source": source, "entity_type": entity_type},
                )

    def _keys_in_metadata(self) -> set[str]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT key FROM public.metadata WHERE metadata_content->>'source_name' IN (:s, :o)"),
                {"s": self.source, "o": self.other_source},
            ).all()
        return {r[0] for r in rows}

    def _sources_and_types_in_embeddings(self) -> set[tuple[str, str]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT metadata_->>'source_name', metadata_->>'entity_type' "
                    "FROM public.data_embeddings WHERE metadata_->>'source_name' IN (:s, :o)"
                ),
                {"s": self.source, "o": self.other_source},
            ).all()
        return {(r[0], r[1]) for r in rows}

    def test_filtered_wipe_only_removes_matching_entity_type(self):
        from scripts.wipe_ingested import wipe

        @contextmanager
        def _session_ctx():
            session = self.Session()
            try:
                yield session
                session.commit()
            finally:
                session.close()

        with patch.dict(wipe.__globals__, {"get_db_session": _session_ctx}):
            wipe(self.source, "entity_type", "notes")

        self.assertEqual(
            self._keys_in_metadata(),
            {f"{self.source}:deals:1", f"{self.other_source}:notes:1"},
        )
        self.assertEqual(
            self._sources_and_types_in_embeddings(),
            {(self.source, "deals"), (self.other_source, "notes")},
        )


if __name__ == "__main__":
    unittest.main()
