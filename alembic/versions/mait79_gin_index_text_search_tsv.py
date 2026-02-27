"""add GIN index on text_search_tsv for full-text search

Add a GIN index on data_embeddings.text_search_tsv so that @@ and tsquery
full-text search operations are performant instead of sequential scans.

Revision ID: mait79_gin
Revises: mait79_simple
Create Date: 2026-02-27

"""
from typing import Sequence, Union

from alembic import op


revision: str = "mait79_gin"
down_revision: Union[str, Sequence[str], None] = "mait79_simple"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create GIN index on text_search_tsv for FTS queries."""
    op.create_index(
        "idx_data_embeddings_text_search_tsv",
        "data_embeddings",
        ["text_search_tsv"],
        schema="public",
        postgresql_using="gin",
    )


def downgrade() -> None:
    """Drop GIN index on text_search_tsv."""
    op.drop_index(
        "idx_data_embeddings_text_search_tsv",
        table_name="data_embeddings",
        schema="public",
    )
