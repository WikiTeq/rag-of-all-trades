"""make text_search_tsv a generated column

Converts the plain tsvector column text_search_tsv into a GENERATED ALWAYS
AS STORED column so that it is automatically kept in sync with the text
column without application-level triggers.

DEPLOYMENT RISK — table lock:
    ALTER TABLE ... DROP COLUMN / ADD COLUMN on a table with existing rows
    acquires an ACCESS EXCLUSIVE lock for the full duration of the rewrite.
    On a large data_embeddings table this will block all reads and writes
    until the migration completes.  Consider running during a maintenance
    window or against a table with few rows.

Revision ID: 416bd1e5f60a
Revises: 1a19b9367fce
Create Date: 2026-02-27 15:00:18.284513

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '416bd1e5f60a'
down_revision: Union[str, Sequence[str], None] = '1a19b9367fce'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Replace plain tsvector column with a generated one."""
    op.drop_column('data_embeddings', 'text_search_tsv', schema='public')
    op.add_column('data_embeddings', sa.Column(
        'text_search_tsv',
        postgresql.TSVECTOR(),
        sa.Computed("to_tsvector('english', text)", persisted=True),
    ), schema='public')


def downgrade() -> None:
    """Revert to plain nullable tsvector column."""
    op.drop_column('data_embeddings', 'text_search_tsv', schema='public')
    op.add_column('data_embeddings', sa.Column(
        'text_search_tsv',
        postgresql.TSVECTOR(),
        nullable=True,
    ), schema='public')
