import logging
from functools import wraps

from fastapi import APIRouter, Depends, HTTPException, Request

from api.dependencies import require_api_key
from utils.api import format_chunks
from utils.config import settings

from .modules import RAGQueryEngine
from .schema import QueryRequest, QueryResponse, SourceReference

router = APIRouter(prefix="/query", tags=["Applications APIs"])
logger = logging.getLogger(__name__)


# Dependency to get RAG engine
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
            limited_func = request.app.state.limiter.limit(settings.env.CHUNK_RATE_LIMIT)(func)

        return await limited_func(*args, **kwargs)

    return wrapper


# Query endpoint
@router.post("/", response_model=QueryResponse)
@limit
async def query_endpoint(
    request: Request,
    payload: QueryRequest,
    rag_engine: RAGQueryEngine = Depends(get_rag_engine),
    _auth: None = Depends(require_api_key),
):
    """
    Retrieve top-k chunks from vector store without LLM answer.
    """
    try:
        # Retrieve top-k nodes directly from vector store
        metadata_filters = payload.metadata_filters or {}

        nodes_with_score = rag_engine.retrieve_top_k(
            query=payload.query, top_k=payload.top_k, metadata=metadata_filters
        )

        chunks = format_chunks(nodes_with_score)

        # Build source references from retrieved nodes
        source_refs = RAGQueryEngine.build_references(nodes_with_score)

        # Return response
        return QueryResponse(references=[SourceReference(**r) for r in source_refs], raw=chunks)

    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error in query: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Unexpected error processing query: {payload.query}")
        raise HTTPException(status_code=500, detail=f"Failed to process query: {str(e)}")
