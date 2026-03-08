"""use 'simple' regconfig for text_search_tsv (language-agnostic)

Switch text_search_tsv from to_tsvector('english', text) to
to_tsvector('simple', text). The 'simple' configuration skips stemming
and stopword removal, making it safe for multilingual content and
consistent regardless of the database locale.

NOTE: This migration drops and recreates the text_search_tsv generated
column, which acquires an ACCESS EXCLUSIVE lock on data_embeddings for
the duration. On large tables this may cause downtime — consider
running during a maintenance window.

Revision ID: a3f2c91b4d8e
Revises: 5feb1e3a07ce
Create Date: 2026-03-08

"""
from typing import Sequence, Union

from alembic import op


revision: str = "a3f2c91b4d8e"
down_revision: Union[str, Sequence[str], None] = "5feb1e3a07ce"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Switch text_search_tsv generated expression to use 'simple' regconfig."""
    op.execute(
        """
        ALTER TABLE public.data_embeddings
        DROP COLUMN text_search_tsv
        """
    )
    op.execute(
        """
        ALTER TABLE public.data_embeddings
        ADD COLUMN text_search_tsv tsvector
            GENERATED ALWAYS AS (to_tsvector('simple', text)) STORED
        """
    )
    # Recreate the GIN index that was dropped with the column
    op.execute(
        """
        CREATE INDEX idx_data_embeddings_text_search_tsv
        ON public.data_embeddings
        USING gin (text_search_tsv)
        """
    )


def downgrade() -> None:
    """Revert text_search_tsv generated expression back to 'english' regconfig."""
    op.execute(
        """
        ALTER TABLE public.data_embeddings
        DROP COLUMN text_search_tsv
        """
    )
    op.execute(
        """
        ALTER TABLE public.data_embeddings
        ADD COLUMN text_search_tsv tsvector
            GENERATED ALWAYS AS (to_tsvector('english', text)) STORED
        """
    )
    op.execute(
        """
        CREATE INDEX idx_data_embeddings_text_search_tsv
        ON public.data_embeddings
        USING gin (text_search_tsv)
        """
    )
