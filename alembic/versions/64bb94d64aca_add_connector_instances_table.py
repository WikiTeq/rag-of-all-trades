"""add connector instances table

Revision ID: 64bb94d64aca
Revises: 5feb1e3a07ce
Create Date: 2026-03-24 16:22:20.134568

NOTE: Generated via `alembic revision --autogenerate` inside the Docker Compose
environment (PostgreSQL is only reachable as 'postgres' inside Docker, not from
the host). The trigger for updated_at is added manually via op.execute() since
Alembic autogenerate does not support Postgres triggers.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "64bb94d64aca"
down_revision: str | Sequence[str] | None = "5feb1e3a07ce"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "connector_instances",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("schedule", sa.Integer(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column(
            "config",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'"),
        ),
        sa.Column("secret", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.CheckConstraint("schedule > 0", name="ck_connector_instance_schedule_positive"),
        sa.UniqueConstraint("type", "name", name="uq_connector_instance_type_name"),
        sa.PrimaryKeyConstraint("id"),
    )

    op.execute("""
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER connector_instances_updated_at
BEFORE UPDATE ON connector_instances
FOR EACH ROW EXECUTE FUNCTION update_updated_at();
""")


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS connector_instances_updated_at ON connector_instances")
    op.execute("DROP FUNCTION IF EXISTS update_updated_at")
    op.drop_table("connector_instances")
