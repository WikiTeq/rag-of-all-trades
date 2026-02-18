from typing import Any, Dict, Optional

from fastapi import FastAPI
from fastmcp import FastMCP
from fastmcp.server.auth import StaticTokenVerifier

from api.v1.chunk_retrieval.modules import RAGQueryEngine
from utils.llm_embedding import llm


def _validate_query(query: str) -> None:
    if not query or not query.strip():
        raise ValueError("Query cannot be empty")


def _validate_top_k(top_k: int) -> None:
    if top_k < 1 or top_k > 20:
        raise ValueError("top_k must be between 1 and 20")


def _format_chunks(nodes_with_score: list[Any]) -> list[str]:
    chunks: list[str] = []
    for node_with_score in nodes_with_score:
        score = node_with_score.score
        if score is None:
            chunks.append(f"Score: n/a | Text: {node_with_score.node.get_text()}")
            continue
        chunks.append(f"Score: {score:.4f} | Text: {node_with_score.node.get_text()}")
    return chunks


def retrieve_chunks_response(
    rag_engine: RAGQueryEngine,
    query: str,
    top_k: int = 5,
    metadata_filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    _validate_query(query)
    _validate_top_k(top_k)
    nodes_with_score = rag_engine.retrieve_top_k(
        query=query,
        top_k=top_k,
        metadata=metadata_filters or {},
    )
    return {
        "references": RAGQueryEngine.build_references(nodes_with_score),
        "raw": _format_chunks(nodes_with_score),
    }


def rephrase_chunks_response(
    rag_engine: RAGQueryEngine,
    query: str,
    top_k: int = 5,
) -> Dict[str, Any]:
    _validate_query(query)
    _validate_top_k(top_k)
    if llm is None:
        raise RuntimeError(
            "LLM is not configured. Please set OPENAI_API_KEY and LLM model name."
        )

    nodes_with_score = rag_engine.retrieve_top_k(query=query, top_k=top_k)
    if not nodes_with_score:
        return {"answer": "No relevant content found.", "references": []}

    chunks_text = "\n\n".join(node.node.get_text() for node in nodes_with_score)
    rephrase_prompt = (
        f'"""Original Query: {query}\n\nRephrase the following content clearly and '
        f'concisely:\n\n{chunks_text}"""'
    )
    llm_response = llm.complete(rephrase_prompt)
    return {
        "answer": str(llm_response),
        "references": RAGQueryEngine.build_references(nodes_with_score),
    }


def create_mcp_server(app: FastAPI, api_key: str) -> FastMCP:
    token = api_key or "__mcp_api_key_not_configured__"
    auth = StaticTokenVerifier(
        tokens={
            token: {
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
    def retrieve_chunks(
        query: str,
        top_k: int = 5,
        metadata_filters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return retrieve_chunks_response(
            rag_engine=get_rag_engine(),
            query=query,
            top_k=top_k,
            metadata_filters=metadata_filters,
        )

    @mcp.tool(
        name="rephrase_chunks",
        description="Generate concise answer from top-k chunks using configured LLM.",
    )
    def rephrase_chunks(query: str, top_k: int = 5) -> Dict[str, Any]:
        return rephrase_chunks_response(
            rag_engine=get_rag_engine(),
            query=query,
            top_k=top_k,
        )

    return mcp
