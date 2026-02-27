"""use simple regconfig for text_search_tsv (multilingual)

Use PostgreSQL 'simple' text search configuration for text_search_tsv so that
full-text search works for non-English content. The 'simple' configuration does
not apply language-specific stemming or stop words, making it safe for
multilingual documents.

Revision ID: mait79_simple
Revises: 416bd1e5f60a
Create Date: 2026-02-27

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "mait79_simple"
down_revision: Union[str, Sequence[str], None] = "416bd1e5f60a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Switch text_search_tsv from 'english' to 'simple' regconfig."""
    op.drop_column("data_embeddings", "text_search_tsv", schema="public")
    op.add_column(
        "data_embeddings",
        sa.Column(
            "text_search_tsv",
            postgresql.TSVECTOR(),
            sa.Computed("to_tsvector('simple', text)", persisted=True),
        ),
        schema="public",
    )


def downgrade() -> None:
    """Revert text_search_tsv to 'english' regconfig."""
    op.drop_column("data_embeddings", "text_search_tsv", schema="public")
    op.add_column(
        "data_embeddings",
        sa.Column(
            "text_search_tsv",
            postgresql.TSVECTOR(),
            sa.Computed("to_tsvector('english', text)", persisted=True),
        ),
        schema="public",
    )
