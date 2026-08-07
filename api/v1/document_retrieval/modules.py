from sqlalchemy import Numeric, cast
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session

from models.embedding import DataEmbeddings

from .schema import ChunkOut, DocumentOut, MetadataFilterInput


def _apply_filter(query, f: MetadataFilterInput):
    col = DataEmbeddings.metadata_
    key = f.name
    val = f.value
    op = f.operator

    json_text = col[key].astext

    if op == "==":
        return query.filter(json_text == str(val))
    if op == "!=":
        return query.filter(json_text != str(val))
    if op == ">":
        return query.filter(cast(json_text, Numeric) > val)
    if op == ">=":
        return query.filter(cast(json_text, Numeric) >= val)
    if op == "<":
        return query.filter(cast(json_text, Numeric) < val)
    if op == "<=":
        return query.filter(cast(json_text, Numeric) <= val)
    if op == "in":
        values = val if isinstance(val, list) else [val]
        return query.filter(json_text.in_([str(v) for v in values]))
    if op == "nin":
        values = val if isinstance(val, list) else [val]
        return query.filter(json_text.notin_([str(v) for v in values]))
    if op == "text_match":
        return query.filter(json_text.like(f"%{val}%"))
    if op == "text_match_insensitive":
        return query.filter(json_text.ilike(f"%{val}%"))
    if op == "contains":
        return query.filter(col[key].cast(JSONB).contains(cast(val, JSONB)))

    raise ValueError(f"Unhandled operator '{op}'")


def _build_chunk(row: DataEmbeddings, max_chunk_length: int) -> ChunkOut:
    txt = row.text or ""
    return ChunkOut(
        text=txt[:max_chunk_length],
        metadata=row.metadata_ or {},
    )


class DocumentRetriever:
    def __init__(self, db: Session):
        self.db = db

    def get_document(
        self,
        document_id: str,
        max_chunks: int,
        max_chunk_length: int,
    ) -> DocumentOut | None:
        rows = (
            self.db.query(DataEmbeddings)
            .filter(DataEmbeddings.metadata_["ref_doc_id"].astext == document_id)
            .limit(max_chunks)
            .all()
        )

        if not rows:
            return None

        doc_metadata = rows[0].metadata_ or {}
        chunks = [_build_chunk(r, max_chunk_length) for r in rows]
        return DocumentOut(document_id=document_id, metadata=doc_metadata, chunks=chunks)

    def get_documents(
        self,
        filters: list[MetadataFilterInput],
        max_chunks: int,
        max_chunk_length: int,
        max_documents: int,
    ) -> list[DocumentOut]:
        # Step 1: fetch distinct ref_doc_id values matching filters
        id_query = self.db.query(DataEmbeddings.metadata_["ref_doc_id"].astext.label("ref_doc_id")).distinct()

        for f in filters:
            id_query = _apply_filter(id_query, f)

        doc_ids = [row.ref_doc_id for row in id_query.limit(max_documents).all() if row.ref_doc_id is not None]

        if not doc_ids:
            return []

        # Step 2: for each document, fetch up to max_chunks rows
        results = []
        for doc_id in doc_ids:
            rows = (
                self.db.query(DataEmbeddings)
                .filter(DataEmbeddings.metadata_["ref_doc_id"].astext == doc_id)
                .limit(max_chunks)
                .all()
            )
            if not rows:
                continue
            doc_metadata = rows[0].metadata_ or {}
            chunks = [_build_chunk(r, max_chunk_length) for r in rows]
            results.append(DocumentOut(document_id=doc_id, metadata=doc_metadata, chunks=chunks))

        return results
