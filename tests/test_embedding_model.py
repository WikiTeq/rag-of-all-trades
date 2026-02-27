"""Integration tests for DataEmbeddings model and generated columns."""
import unittest

from sqlalchemy.exc import OperationalError

from models.embedding import DataEmbeddings
from utils.db import SessionLocal


class TestDataEmbeddingsGeneratedColumns(unittest.TestCase):
    """Test that text_search_tsv is auto-populated by PostgreSQL."""

    def setUp(self) -> None:
        self.session = SessionLocal()

    def tearDown(self) -> None:
        self.session.rollback()
        self.session.close()

    def test_text_search_tsv_auto_populated_on_insert(self) -> None:
        """Inserting a row populates text_search_tsv via the generated column."""
        try:
            row = DataEmbeddings(
                id=9192918293847,
                text="Hello world",
                metadata_={"key": "test-key", "checksum": "test-checksum"},
            )
            self.session.add(row)
            self.session.flush()
            self.session.refresh(row)
        except OperationalError as e:
            self.skipTest(f"Database not available: {e}")

        self.assertIsNotNone(
            row.text_search_tsv,
            "text_search_tsv should be populated by PostgreSQL generated column",
        )
        tsv_str = str(row.text_search_tsv).lower()
        self.assertIn("hello", tsv_str, "tsvector should contain 'hello'")
        self.assertIn("world", tsv_str, "tsvector should contain 'world'")
