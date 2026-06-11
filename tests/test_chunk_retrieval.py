from unittest.mock import Mock, patch

import pytest
from llama_index.core.vector_stores.types import FilterCondition
from pydantic import TypeAdapter, ValidationError

from api.v1.chunk_retrieval import routes
from api.v1.chunk_retrieval.modules import _OPERATOR_MAP, RAGQueryEngine
from api.v1.chunk_retrieval.schema import MetadataFilterItem, QueryRequest

_filter_adapter = TypeAdapter(MetadataFilterItem)


class _DummyNode:
    def __init__(self, text: str = ""):
        self._text = text
        self.metadata = {}

    def get_text(self):
        return self._text

    def get_content(self):
        return self._text


class _DummyNodeWithScore:
    def __init__(self, text: str = "", score: float = 0.9):
        self.node = _DummyNode(text)
        self.score = score


def _make_engine():
    vector_store = Mock()
    return RAGQueryEngine(vector_store=vector_store)


class TestMetadataFilterItemSchema:
    @pytest.mark.parametrize("operator", ["EQ", "NE", "GT", "GTE", "LT", "LTE", "TEXT_MATCH"])
    def test_scalar_operators_accept_scalar_value(self, operator):
        item = _filter_adapter.validate_python({"name": "field", "operator": operator, "value": "val"})
        assert item.operator == operator

    @pytest.mark.parametrize("operator", ["IN", "NIN"])
    def test_list_operators_accept_list_value(self, operator):
        item = _filter_adapter.validate_python({"name": "field", "operator": operator, "value": ["a", "b"]})
        assert item.value == ["a", "b"]

    @pytest.mark.parametrize("operator", ["IN", "NIN"])
    def test_list_operators_reject_scalar_value(self, operator):
        with pytest.raises(ValidationError):
            _filter_adapter.validate_python({"name": "field", "operator": operator, "value": "scalar"})

    @pytest.mark.parametrize("operator", ["EQ", "NE", "GT", "GTE", "LT", "LTE", "TEXT_MATCH"])
    def test_scalar_operators_reject_list_value(self, operator):
        with pytest.raises(ValidationError):
            _filter_adapter.validate_python({"name": "field", "operator": operator, "value": ["a", "b"]})

    def test_invalid_operator_raises(self):
        with pytest.raises(ValidationError):
            _filter_adapter.validate_python({"name": "field", "operator": "INVALID", "value": "val"})


class TestQueryRequestSchema:
    def test_metadata_filters_none_by_default(self):
        req = QueryRequest(query="test")
        assert req.metadata_filters is None

    def test_metadata_filters_accepts_list_of_items(self):
        req = QueryRequest(
            query="test",
            metadata_filters=[{"name": "project", "operator": "EQ", "value": "MAIT"}],
        )
        assert len(req.metadata_filters) == 1
        assert req.metadata_filters[0].operator == "EQ"

    def test_metadata_filters_rejects_old_dict_form(self):
        with pytest.raises(ValidationError):
            QueryRequest(query="test", metadata_filters={"project": "MAIT"})


class TestBuildFilterObject:
    def test_returns_none_for_none_input(self):
        engine = _make_engine()
        assert engine._build_filter_object(None) is None

    def test_returns_none_for_empty_list(self):
        engine = _make_engine()
        assert engine._build_filter_object([]) is None

    @pytest.mark.parametrize("operator", ["EQ", "NE", "GT", "GTE", "LT", "LTE", "TEXT_MATCH"])
    def test_scalar_operators_map_correctly(self, operator):
        engine = _make_engine()
        item = _filter_adapter.validate_python({"name": "field", "operator": operator, "value": "val"})
        result = engine._build_filter_object([item])
        assert result is not None
        assert result.filters[0].operator == _OPERATOR_MAP[operator]

    @pytest.mark.parametrize("operator", ["IN", "NIN"])
    def test_list_operators_map_correctly(self, operator):
        engine = _make_engine()
        item = _filter_adapter.validate_python({"name": "tags", "operator": operator, "value": ["a", "b"]})
        result = engine._build_filter_object([item])
        assert result.filters[0].operator == _OPERATOR_MAP[operator]

    def test_all_operators_covered_in_map(self):
        expected = {"EQ", "NE", "GT", "GTE", "LT", "LTE", "IN", "NIN", "TEXT_MATCH"}
        assert set(_OPERATOR_MAP.keys()) == expected

    def test_condition_is_and(self):
        engine = _make_engine()
        item = _filter_adapter.validate_python({"name": "f", "operator": "EQ", "value": "v"})
        result = engine._build_filter_object([item])
        assert result.condition == FilterCondition.AND

    def test_multiple_filters_all_included(self):
        engine = _make_engine()
        items = [
            _filter_adapter.validate_python({"name": "project", "operator": "EQ", "value": "MAIT"}),
            _filter_adapter.validate_python({"name": "tags", "operator": "IN", "value": ["A", "B"]}),
            _filter_adapter.validate_python({"name": "date", "operator": "GT", "value": "2026-01-01"}),
        ]
        result = engine._build_filter_object(items)
        assert len(result.filters) == 3


def _make_request(rag_engine):
    limiter_mock = Mock()
    limiter_mock.limit.return_value = lambda f: f
    request = Mock()
    request.app.state.rag_engine = rag_engine
    request.app.state.limiter = limiter_mock
    return request


@pytest.mark.asyncio
async def test_query_endpoint_passes_metadata_filters_to_engine():
    nodes = [_DummyNodeWithScore("content")]
    rag_engine = Mock()
    rag_engine.retrieve_top_k.return_value = nodes

    payload = Mock()
    payload.query = "test"
    payload.top_k = 5
    payload.metadata_filters = [_filter_adapter.validate_python({"name": "project", "operator": "EQ", "value": "MAIT"})]

    with patch("api.v1.chunk_retrieval.routes.format_chunks", return_value=[]):
        await routes.query_endpoint(
            request=_make_request(rag_engine),
            payload=payload,
            rag_engine=rag_engine,
        )

    rag_engine.retrieve_top_k.assert_called_once_with(query="test", top_k=5, metadata=payload.metadata_filters)


@pytest.mark.asyncio
async def test_query_endpoint_passes_empty_list_when_no_filters():
    nodes = [_DummyNodeWithScore("content")]
    rag_engine = Mock()
    rag_engine.retrieve_top_k.return_value = nodes

    payload = Mock()
    payload.query = "test"
    payload.top_k = 5
    payload.metadata_filters = None

    with patch("api.v1.chunk_retrieval.routes.format_chunks", return_value=[]):
        await routes.query_endpoint(
            request=_make_request(rag_engine),
            payload=payload,
            rag_engine=rag_engine,
        )

    rag_engine.retrieve_top_k.assert_called_once_with(query="test", top_k=5, metadata=[])
