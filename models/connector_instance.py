import copy

from sqlalchemy import Boolean, CheckConstraint, Column, DateTime, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from utils.db import Base


class ConnectorInstance(Base):
    """ORM model for a connector instance."""

    __tablename__ = "connector_instances"

    id = Column(Integer, primary_key=True)
    type = Column(String, nullable=False)  # plain string: "jira", "s3", etc.
    name = Column(String, nullable=False)
    schedule = Column(Integer, nullable=False)  # seconds between runs
    enabled = Column(Boolean, nullable=False, default=True)
    config = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'"))
    # default=dict (callable) avoids sharing a mutable {} across instances at the Python layer.
    # server_default=text('{}') ensures raw SQL inserts also get a valid default.
    secret = Column(Text, nullable=True)  # Fernet-encrypted JSON string; decrypted at read time
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now())
    # NOTE: updated_at is maintained by a Postgres trigger added in the Alembic migration,
    # NOT by SQLAlchemy onupdate= (which only fires on Core UPDATE, not ORM).

    __table_args__ = (
        CheckConstraint("schedule > 0", name="ck_connector_instance_schedule_positive"),
        UniqueConstraint("type", "name", name="uq_connector_instance_type_name"),
    )

    def to_config(self) -> dict:
        """Return a deep copy of the instance config dict safe for mutation by callers."""
        return {
            "type": self.type,
            "name": self.name,
            "schedule": self.schedule,
            "config": copy.deepcopy(self.config),
        }
