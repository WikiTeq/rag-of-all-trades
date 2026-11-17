from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.sql import func

from utils.db import Base


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"

    id = Column(Integer, primary_key=True)
    connector_name = Column(String, nullable=False, index=True)
    connector_type = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False, index=True)
    items_ingested = Column(Integer, nullable=False, default=0)
    items_skipped = Column(Integer, nullable=False, default=0)
    started_at = Column(DateTime(timezone=True), nullable=False, index=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    duration_ms = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
