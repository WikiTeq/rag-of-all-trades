import sqlalchemy as sa
from sqlalchemy import Column, BigInteger, Computed, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from pgvector.sqlalchemy import Vector
from sqlalchemy.dialects.postgresql import TSVECTOR
from utils.db import Base

class DataEmbeddings(Base):
    __tablename__ = "data_embeddings"
    __table_args__ = {"schema": "public"}

    id = Column(BigInteger, primary_key=True, nullable=False)
    text = Column(String, nullable=False)
    metadata_ = Column(JSONB, nullable=True)
    node_id = Column(String, nullable=True)
    embedding = Column(Vector, nullable=True)
    text_search_tsv = Column(TSVECTOR, Computed("to_tsvector('english', text)", persisted=True))

    key_text = Column(Text, sa.Computed("metadata_ ->> 'key'", persisted=True))
    checksum_text = Column(Text, sa.Computed("metadata_ ->> 'checksum'", persisted=True))
