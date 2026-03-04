"""add GIN index on text_search_tsv for full-text search

Add a GIN index on data_embeddings.text_search_tsv so that @@ and tsquery
full-text search operations are performant. Verified against LlamaIndex
PGVectorStore schema (get_data_model with hybrid_search=True), which creates
the same GIN index on text_search_tsv when it creates the table.

Revision ID: 5feb1e3a07ce
Revises: 416bd1e5f60a
Create Date: 2026-02-27

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "5feb1e3a07ce"
down_revision: Union[str, Sequence[str], None] = "416bd1e5f60a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create GIN index on text_search_tsv and btree index on ref_doc_id.

    Matches PGVectorStore hybrid_search schema.
    """
    op.create_index(
        "idx_data_embeddings_text_search_tsv",
        "data_embeddings",
        ["text_search_tsv"],
        schema="public",
        postgresql_using="gin",
    )
    op.create_index(
        "embeddings_idx_1",
        "data_embeddings",
        [sa.text("(metadata_->>'ref_doc_id')")],
        schema="public",
        postgresql_using="btree",
    )


def downgrade() -> None:
    """Drop GIN index on text_search_tsv and btree index on ref_doc_id."""
    op.drop_index(
        "embeddings_idx_1",
        table_name="data_embeddings",
        schema="public",
    )
    op.drop_index(
        "idx_data_embeddings_text_search_tsv",
        table_name="data_embeddings",
        schema="public",
    )
