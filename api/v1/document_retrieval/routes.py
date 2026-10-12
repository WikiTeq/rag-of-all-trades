import logging
from collections.abc import Generator

from fastapi import APIRouter, Depends, HTTPException, Request

from api.dependencies import require_api_key
from api.v1.utils import limit
from utils.config import settings
from utils.db import SessionLocal

from .modules import DocumentRetriever
from .schema import DocumentOut, DocumentRequest, DocumentsRequest

router = APIRouter(prefix="/document", tags=["Document Retrieval"])
logger = logging.getLogger(__name__)


def _get_retriever() -> Generator[DocumentRetriever, None, None]:
    db = SessionLocal()
    try:
        yield DocumentRetriever(db)
    finally:
        db.close()


@router.post("", response_model=DocumentOut, dependencies=[Depends(require_api_key)])
@limit(settings.env.CHUNK_RATE_LIMIT)
async def get_document(
    request: Request,
    payload: DocumentRequest,
    retriever: DocumentRetriever = Depends(_get_retriever),
):
    logger.info("document retrieval: document_id=%s", payload.document_id)
    try:
        result = retriever.get_document(
            document_id=payload.document_id,
            max_chunks=payload.max_chunks,
            max_chunk_length=payload.max_chunk_length,
        )
        if result is None:
            raise HTTPException(status_code=404, detail=f"Document '{payload.document_id}' not found")
        return result
    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Validation error in document retrieval: %s", str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Unexpected error retrieving document: %s", payload.document_id)
        raise HTTPException(status_code=500, detail="Failed to retrieve document")


@router.post("s", response_model=list[DocumentOut], dependencies=[Depends(require_api_key)])
@limit(settings.env.CHUNK_RATE_LIMIT)
async def get_documents(
    request: Request,
    payload: DocumentsRequest,
    retriever: DocumentRetriever = Depends(_get_retriever),
):
    logger.info("documents retrieval: filters=%d", len(payload.metadata_filters))
    try:
        return retriever.get_documents(
            filters=payload.metadata_filters,
            max_chunks=payload.max_chunks,
            max_chunk_length=payload.max_chunk_length,
            max_documents=payload.max_documents,
        )
    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Validation error in documents retrieval: %s", str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Unexpected error retrieving documents")
        raise HTTPException(status_code=500, detail="Failed to retrieve documents")
