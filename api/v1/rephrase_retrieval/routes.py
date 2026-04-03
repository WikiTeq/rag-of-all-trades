import logging
from functools import wraps

from fastapi import APIRouter, Depends, HTTPException, Request
from llama_index.core.llms import ChatMessage, MessageRole

from api.v1.chunk_retrieval.modules import RAGQueryEngine
from utils.config import settings
from utils.llm_embedding import llm

from .schema import QueryRequest, QueryResponse, SourceReference

router = APIRouter(prefix="/rephrase", tags=["Applications APIs"])
logger = logging.getLogger(__name__)


# Dependency to get RAG engine
def get_rag_engine(request: Request) -> RAGQueryEngine:
    if not hasattr(request.app.state, "rag_engine"):
        raise HTTPException(status_code=503, detail="RAG engine not initialized")

    # Validate LLM BEFORE we build engine
    if llm is None:
        raise HTTPException(
            status_code=503, detail="LLM is not configured. Please set OPENAI_API_KEY and LLM model name."
        )

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
            limited_func = request.app.state.limiter.limit(settings.env.REPHRASE_RATE_LIMIT)(func)

        return await limited_func(*args, **kwargs)

    return wrapper


@router.post("/", response_model=QueryResponse)
@limit
async def query_endpoint(request: Request, payload: QueryRequest, rag_engine: RAGQueryEngine = Depends(get_rag_engine)):
    """
    Rephrase top-k chunks using LLM.
    """
    try:
        # Validate query
        if not payload.query or not payload.query.strip():
            raise HTTPException(status_code=400, detail="Query cannot be empty")

        nodes_with_score = rag_engine.retrieve_top_k(query=payload.query, top_k=payload.top_k)

        if not nodes_with_score:
            return QueryResponse(answer="No relevant content found.", references=[])

        chunks_text = "\n\n".join([n.node.get_text() for n in nodes_with_score])
        messages = [
            ChatMessage(
                role=MessageRole.SYSTEM,
                content="Rephrase the following content clearly and concisely.",
            ),
            ChatMessage(
                role=MessageRole.USER,
                content=f"Query: {payload.query}\n\nContent:\n\n{chunks_text}",
            ),
        ]
        llm_response = llm.chat(messages)

        source_refs = RAGQueryEngine.build_references(nodes_with_score)

        return QueryResponse(
            answer=llm_response.message.content, references=[SourceReference(**r) for r in source_refs]
        )

    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error in rephrase query: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Unexpected error in rephrase endpoint: {payload.query}")
        raise HTTPException(status_code=500, detail=f"Failed to process rephrase query: {str(e)}")
