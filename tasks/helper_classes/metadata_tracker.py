from models.metadata import MetaData
from models.embedding import DataEmbeddings
from sqlalchemy import delete
from utils.db import get_db_session

#MetadataTracker
class MetadataTracker:
    def __init__(self):
        pass

    def get_latest_record(self, key: str):
        with get_db_session() as db:
            row = (
                db.query(MetaData.checksum, MetaData.version)
                .filter(MetaData.key == key)
                .order_by(MetaData.version.desc())
                .first()
            )
            return row

    def record_metadata(self, key, checksum, version, chunks, last_modified, extra_metadata=None):
        with get_db_session() as db:
            meta_entry = MetaData(
                key=key,
                checksum=checksum,
                version=version,
                metadata_content={"chunks": chunks, "source": "generic", **(extra_metadata or {})},
                last_modified=last_modified,
            )
            db.add(meta_entry)
            # Commit handled by context manager

    def delete_previous_embeddings(self, key: str):
        with get_db_session() as db:
            stmt = delete(DataEmbeddings).where(DataEmbeddings.key_text == key)
            db.execute(stmt)
            # Commit handled by context manager