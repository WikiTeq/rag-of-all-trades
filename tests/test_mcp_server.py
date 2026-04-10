from unittest.mock import Mock, patch

import pytest
from fastapi import FastAPI

from api import mcp_server


class _DummyNode:
    def __init__(self, text: str, metadata=None):
        self._text = text
        self.metadata = metadata or {}

    def get_text(self):
        return self._text

    def get_content(self):
        return self._text


class _DummyNodeWithScore:
    def __init__(self, text: str, score: float = 0.7, metadata=None):
        self.node = _DummyNode(text=text, metadata=metadata or {})
        self.score = score


@pytest.mark.asyncio
async def test_retrieve_chunks_response_returns_expected_shape():
    rag_engine = Mock()
    rag_engine.retrieve_top_k.return_value = [
        _DummyNodeWithScore(
            text="Chunk text",
            score=0.9,
            metadata={"source_name": "docs", "file_name": "a.md"},
        )
    ]

    result = await mcp_server.retrieve_chunks_response(
        rag_engine=rag_engine,
        query="hello",
        top_k=3,
        metadata_filters={"source_name": "docs"},
    )

    rag_engine.retrieve_top_k.assert_called_once_with(
        query="hello",
        top_k=3,
        metadata={"source_name": "docs"},
    )
    assert "references" in result
    assert "raw" in result
    assert len(result["references"]) == 1
    assert result["raw"][0].startswith("Score: 0.9000 | Text: Chunk text")


@pytest.mark.asyncio
async def test_retrieve_chunks_response_calls_engine():
    rag_engine = Mock()
    rag_engine.retrieve_top_k.return_value = []
    result = await mcp_server.retrieve_chunks_response(rag_engine=rag_engine, query="test", top_k=5)
    assert result == {"references": [], "raw": []}


@pytest.mark.asyncio
async def test_rephrase_chunks_response_without_results():
    rag_engine = Mock()
    rag_engine.retrieve_top_k.return_value = []

    with patch.object(mcp_server, "llm", Mock()):
        result = await mcp_server.rephrase_chunks_response(
            rag_engine=rag_engine,
            query="hello",
        )

    assert result == {"answer": "No relevant content found.", "references": []}


@pytest.mark.asyncio
async def test_rephrase_chunks_response_requires_llm():
    rag_engine = Mock()
    rag_engine.retrieve_top_k.return_value = [_DummyNodeWithScore(text="content", score=0.5)]

    with patch.object(mcp_server, "llm", None):
        with pytest.raises(RuntimeError):
            await mcp_server.rephrase_chunks_response(rag_engine=rag_engine, query="hello")


@pytest.mark.asyncio
async def test_rephrase_chunks_response_success():
    rag_engine = Mock()
    rag_engine.retrieve_top_k.return_value = [
        _DummyNodeWithScore(
            text="Some content",
            score=0.5,
            metadata={"source_name": "docs", "file_name": "note.md"},
        )
    ]
    llm_mock = Mock()
    llm_mock.chat.return_value = Mock(message=Mock(content="Rephrased output"))

    with patch.object(mcp_server, "llm", llm_mock):
        result = await mcp_server.rephrase_chunks_response(
            rag_engine=rag_engine,
            query="question",
            top_k=2,
        )

    rag_engine.retrieve_top_k.assert_called_once_with(query="question", top_k=2)
    assert result["answer"] == "Rephrased output"
    assert len(result["references"]) == 1


def test_create_mcp_server_builds_instance():
    app = FastAPI()
    mcp = mcp_server.create_mcp_server(app=app, api_key="test-key")
    assert mcp is not None


def test_create_mcp_server_rejects_empty_api_key():
    app = FastAPI()
    with pytest.raises(ValueError):
        mcp_server.create_mcp_server(app=app, api_key="")
