from fastapi import APIRouter, Depends, Request, HTTPException
from typing import List
import logging
from functools import wraps
from .schema import QueryRequest, QueryResponse, SourceReference
from .modules import RAGQueryEngine
from utils.config import settings

router = APIRouter(prefix="/query", tags=["Applications APIs"])
logger = logging.getLogger(__name__)

#Dependency to get RAG engine
def get_rag_engine(request: Request) -> RAGQueryEngine:
    if not hasattr(request.app.state, "rag_engine"):
        raise HTTPException(status_code=503, detail="RAG engine not initialized")
    return request.app.state.rag_engine

# Conditional Decorator helper for api rate limiting
def limit(func):
    if not settings.env.ENABLE_RATE_LIMIT:
        return func

    limited_func = None

    @wraps(func)
    async def wrapper(*args, **kwargs):
        nonlocal limited_func
        request: Request = kwargs.get("request") or args[0]
        if limited_func is None:
            limited_func = request.app.state.limiter.limit(
                settings.env.CHUNK_RATE_LIMIT
            )(func)

        return await limited_func(*args, **kwargs)

    return wrapper

#Query endpoint
@router.post("/", response_model=QueryResponse)
@limit
async def query_endpoint(
    request: Request,
    payload: QueryRequest,
    rag_engine: RAGQueryEngine = Depends(get_rag_engine)
):
    """
    Retrieve top-k chunks from vector store without LLM answer.
    """
    try:
        # Validate query
        if not payload.query or not payload.query.strip():
            raise HTTPException(status_code=400, detail="Query cannot be empty")

        # Retrieve top-k nodes directly from vector store
        metadata_filters = payload.metadata_filters or {}

        nodes_with_score = rag_engine.retrieve_top_k(
            query=payload.query,
            top_k=payload.top_k,
            metadata=metadata_filters
        )

        # Format chunks as list of strings with optional scores
        chunks: List[str] = [
            f"Score: {n.score:.4f} | Text: {n.node.get_text()}"
            for n in nodes_with_score
        ]

        # Build source references from retrieved nodes
        source_refs = RAGQueryEngine.build_references(nodes_with_score)

        # Return response
        return QueryResponse(
            references=[SourceReference(**r) for r in source_refs],
            raw=chunks
        )

    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error in query: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Unexpected error processing query: {payload.query}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process query: {str(e)}"
        )