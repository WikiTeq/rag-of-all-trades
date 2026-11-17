"""add ingestion runs table

Revision ID: 2c5cc8f11250
Revises: 5feb1e3a07ce
Create Date: 2026-02-24 03:20:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "2c5cc8f11250"
down_revision: Union[str, Sequence[str], None] = "5feb1e3a07ce"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ingestion_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("connector_name", sa.String(), nullable=False),
        sa.Column("connector_type", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("items_ingested", sa.Integer(), nullable=False),
        sa.Column("items_skipped", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_ingestion_runs_connector_name"), "ingestion_runs", ["connector_name"], unique=False)
    op.create_index(op.f("ix_ingestion_runs_connector_type"), "ingestion_runs", ["connector_type"], unique=False)
    op.create_index(op.f("ix_ingestion_runs_started_at"), "ingestion_runs", ["started_at"], unique=False)
    op.create_index(op.f("ix_ingestion_runs_status"), "ingestion_runs", ["status"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_ingestion_runs_status"), table_name="ingestion_runs")
    op.drop_index(op.f("ix_ingestion_runs_started_at"), table_name="ingestion_runs")
    op.drop_index(op.f("ix_ingestion_runs_connector_type"), table_name="ingestion_runs")
    op.drop_index(op.f("ix_ingestion_runs_connector_name"), table_name="ingestion_runs")
    op.drop_table("ingestion_runs")
