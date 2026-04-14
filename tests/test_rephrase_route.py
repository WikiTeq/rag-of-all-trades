from unittest.mock import AsyncMock, Mock, patch

import pytest

from api.v1.rephrase_retrieval import routes


class _DummyNode:
    def __init__(self, text: str):
        self._text = text
        self.metadata = {}

    def get_text(self):
        return self._text

    def get_content(self):
        return self._text


class _DummyNodeWithScore:
    def __init__(self, text: str, score: float = 0.8):
        self.node = _DummyNode(text)
        self.score = score


@pytest.mark.asyncio
async def test_rephrase_uses_to_thread_for_retrieval_and_achat_for_llm():
    """retrieve_top_k must be called via asyncio.to_thread; llm.achat called directly."""
    nodes = [_DummyNodeWithScore("content")]
    llm_mock = Mock()
    llm_mock.achat = AsyncMock(return_value=Mock(message=Mock(content="answer")))

    to_thread_calls = []

    async def fake_to_thread(func, *args, **kwargs):
        to_thread_calls.append(func)
        return func(*args, **kwargs)

    rag_engine = Mock()
    rag_engine.retrieve_top_k.return_value = nodes

    with (
        patch.object(routes, "llm", llm_mock),
        patch("api.v1.rephrase_retrieval.routes.asyncio.to_thread", side_effect=fake_to_thread),
    ):
        limiter_mock = Mock()
        limiter_mock.limit.return_value = lambda f: f

        request = Mock()
        request.app.state.rag_engine = rag_engine
        request.app.state.limiter = limiter_mock

        payload = Mock()
        payload.query = "test query"
        payload.top_k = 5

        await routes.query_endpoint(request=request, payload=payload, rag_engine=rag_engine)

    assert rag_engine.retrieve_top_k in to_thread_calls
    llm_mock.achat.assert_awaited_once()


@pytest.mark.asyncio
async def test_rephrase_passes_top_k_from_payload():
    """top_k must come from payload, not be hardcoded."""
    nodes = [_DummyNodeWithScore("content")]
    llm_mock = Mock()
    llm_mock.achat = AsyncMock(return_value=Mock(message=Mock(content="answer")))

    rag_engine = Mock()
    rag_engine.retrieve_top_k.return_value = nodes

    with (
        patch.object(routes, "llm", llm_mock),
        patch("api.v1.rephrase_retrieval.routes.asyncio.to_thread", side_effect=lambda f, *a, **kw: f(*a, **kw)),
    ):
        payload = Mock()
        payload.query = "test"
        payload.top_k = 42

        limiter_mock = Mock()
        limiter_mock.limit.return_value = lambda f: f
        request = Mock()
        request.app.state.limiter = limiter_mock

        await routes.query_endpoint(
            request=request,
            payload=payload,
            rag_engine=rag_engine,
        )

    rag_engine.retrieve_top_k.assert_called_once_with(query="test", top_k=42)
