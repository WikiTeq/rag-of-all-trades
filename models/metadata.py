from sqlalchemy import Column, Integer, String, DateTime, JSON, func
from sqlalchemy.sql import func
from utils.db import Base

class MetaData(Base):
    __tablename__ = "metadata"
    id = Column(Integer, primary_key=True, index=True)
    key = Column(String, nullable=False, index=True)
    checksum = Column(String, nullable=False)
    version = Column(Integer, nullable=False, default=1)
    metadata_content = Column(JSON, nullable=True)
    last_modified = Column(DateTime, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())