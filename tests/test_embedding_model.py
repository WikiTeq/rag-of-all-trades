"""Integration test for the DataEmbeddings model.

Verifies that text_search_tsv is automatically populated as a generated
column when a row is inserted.  The test requires a live PostgreSQL
connection and will be skipped if the database is unavailable.
"""

import unittest

DB_AVAILABLE = False
try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import Session
    from utils.config import settings
    from utils.db import Base
    from models.embedding import DataEmbeddings

    _engine = create_engine(settings.env.DATABASE_URL, pool_pre_ping=True)
    with _engine.connect() as _conn:
        _conn.execute(text("SELECT 1"))
    DB_AVAILABLE = True
except Exception:
    pass


@unittest.skipUnless(DB_AVAILABLE, "PostgreSQL database not available")
class TestTextSearchTsvGenerated(unittest.TestCase):
    """Verify text_search_tsv is auto-populated by the database."""

    def setUp(self):
        self.engine = create_engine(settings.env.DATABASE_URL)
        self.session = Session(self.engine)

    def tearDown(self):
        self.session.rollback()
        self.session.close()

    def test_text_search_tsv_is_populated_on_insert(self):
        row = DataEmbeddings(
            text="hello world integration test",
            metadata_={"key": "test-key", "checksum": "abc123"},
        )
        self.session.add(row)
        self.session.flush()
        self.session.refresh(row)

        self.assertIsNotNone(row.text_search_tsv)
        self.assertNotEqual(row.text_search_tsv, "")

    def test_text_search_tsv_contains_expected_lexemes(self):
        row = DataEmbeddings(
            text="the quick brown fox",
            metadata_={"key": "test-key2", "checksum": "def456"},
        )
        self.session.add(row)
        self.session.flush()

        result = self.session.execute(
            text(
                "SELECT text_search_tsv::text FROM public.data_embeddings WHERE id = :id"
            ),
            {"id": row.id},
        ).scalar()

        self.assertIsNotNone(result)
        # 'simple' regconfig preserves words as-is (no stemming)
        self.assertIn("quick", result)
        self.assertIn("fox", result)


if __name__ == "__main__":
    unittest.main()
