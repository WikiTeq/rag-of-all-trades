import asyncio
import logging
from typing import Any

from fastapi import FastAPI
from fastmcp import FastMCP
from fastmcp.server.auth import StaticTokenVerifier
from llama_index.core.llms import ChatMessage, MessageRole

from api.v1.chunk_retrieval.modules import RAGQueryEngine
from utils.api import format_chunks
from utils.llm_embedding import llm

logger = logging.getLogger(__name__)


def _validate_query(query: str) -> None:
    if not query or not query.strip():
        raise ValueError("Query cannot be empty")


def _validate_top_k(top_k: int) -> None:
    if top_k < 1 or top_k > 100:
        raise ValueError("top_k must be between 1 and 100")


async def retrieve_chunks_response(
    rag_engine: RAGQueryEngine,
    query: str,
    top_k: int = 20,
    metadata_filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    _validate_query(query)
    _validate_top_k(top_k)
    logger.info("MCP retrieve_chunks: top_k=%d has_filters=%s", top_k, bool(metadata_filters))
    nodes_with_score = await asyncio.to_thread(
        rag_engine.retrieve_top_k,
        query=query,
        top_k=top_k,
        metadata=metadata_filters or {},
    )
    logger.info("MCP retrieve_chunks: num_results=%d", len(nodes_with_score))
    return {
        "references": RAGQueryEngine.build_references(nodes_with_score),
        "raw": format_chunks(nodes_with_score),
    }


async def rephrase_chunks_response(
    rag_engine: RAGQueryEngine,
    query: str,
    top_k: int = 20,
) -> dict[str, Any]:
    _validate_query(query)
    _validate_top_k(top_k)
    if llm is None:
        raise RuntimeError("LLM is not configured. Please configure the LLM provider, API key, and model name.")

    logger.info("MCP rephrase_chunks: top_k=%d", top_k)
    nodes_with_score = await asyncio.to_thread(
        rag_engine.retrieve_top_k,
        query=query,
        top_k=top_k,
    )
    if not nodes_with_score:
        logger.info("MCP rephrase_chunks: no results found")
        return {"answer": "No relevant content found.", "references": []}

    chunks_text = "\n\n".join(node.node.get_text() for node in nodes_with_score)
    messages = [
        ChatMessage(
            role=MessageRole.SYSTEM,
            content="Rephrase the following content clearly and concisely.",
        ),
        ChatMessage(
            role=MessageRole.USER,
            content=f"Query: {query}\n\nContent:\n\n{chunks_text}",
        ),
    ]
    llm_response = await asyncio.to_thread(llm.chat, messages)
    logger.info("MCP rephrase_chunks: num_results=%d", len(nodes_with_score))
    return {
        "answer": llm_response.message.content,
        "references": RAGQueryEngine.build_references(nodes_with_score),
    }


def create_mcp_server(app: FastAPI, api_key: str) -> FastMCP:
    if not api_key:
        raise ValueError("MCP_API_KEY must be configured. Set it in your .env file or environment variables.")
    auth = StaticTokenVerifier(
        tokens={
            api_key: {
                "client_id": "rag-of-all-trades-client",
                "scopes": [],
            }
        }
    )
    mcp = FastMCP("Rag-of-all-trades MCP", auth=auth)

    def get_rag_engine() -> RAGQueryEngine:
        rag_engine = getattr(app.state, "rag_engine", None)
        if rag_engine is None:
            raise RuntimeError("RAG engine not initialized")
        return rag_engine

    @mcp.tool(
        name="retrieve_chunks",
        description="Retrieve top-k chunks from vector store with optional metadata filters.",
    )
    async def retrieve_chunks(
        query: str,
        top_k: int = 20,
        metadata_filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await retrieve_chunks_response(
            rag_engine=get_rag_engine(),
            query=query,
            top_k=top_k,
            metadata_filters=metadata_filters,
        )

    @mcp.tool(
        name="rephrase_chunks",
        description="Generate concise answer from top-k chunks using configured LLM.",
    )
    async def rephrase_chunks(query: str, top_k: int = 20) -> dict[str, Any]:
        return await rephrase_chunks_response(
            rag_engine=get_rag_engine(),
            query=query,
            top_k=top_k,
        )

    return mcp
