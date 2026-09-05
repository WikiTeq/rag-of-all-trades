from unittest.mock import Mock, patch

import pytest
from llama_index.core.vector_stores.types import FilterCondition, VectorStoreQueryMode
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


def _make_engine(hybrid_search: bool = False):
    vector_store = Mock()
    return RAGQueryEngine(vector_store=vector_store, hybrid_search=hybrid_search)


class TestMetadataFilterItemSchema:
    @pytest.mark.parametrize("operator", ["EQ", "NE", "GT", "GTE", "LT", "LTE", "TEXT_MATCH", "CONTAINS"])
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

    @pytest.mark.parametrize("operator", ["EQ", "NE", "GT", "GTE", "LT", "LTE", "TEXT_MATCH", "CONTAINS"])
    def test_scalar_operators_reject_list_value(self, operator):
        with pytest.raises(ValidationError):
            _filter_adapter.validate_python({"name": "field", "operator": operator, "value": ["a", "b"]})

    def test_invalid_operator_raises(self):
        with pytest.raises(ValidationError):
            _filter_adapter.validate_python({"name": "field", "operator": "INVALID", "value": "val"})

    @pytest.mark.parametrize("name", ["field!", "field;drop", "field\x00", "field#bad"])
    def test_invalid_name_raises(self, name):
        with pytest.raises(ValidationError):
            _filter_adapter.validate_python({"name": name, "operator": "EQ", "value": "val"})

    @pytest.mark.parametrize("name", ["field", "field_name", "field-name", "field.name", "Field123", "field name"])
    def test_valid_name_accepted(self, name):
        item = _filter_adapter.validate_python({"name": name, "operator": "EQ", "value": "val"})
        assert item.name == name

    @pytest.mark.parametrize("value", ["bad\x00value", "field#bad", "val~nope"])
    def test_invalid_scalar_str_value_raises(self, value):
        with pytest.raises(ValidationError):
            _filter_adapter.validate_python({"name": "field", "operator": "EQ", "value": value})

    @pytest.mark.parametrize("value", ["bad\x00value", "field#bad"])
    def test_invalid_list_str_value_raises(self, value):
        with pytest.raises(ValidationError):
            _filter_adapter.validate_python({"name": "field", "operator": "IN", "value": ["ok", value]})

    @pytest.mark.parametrize(
        "value", ["hello world", "key=value", "tag:foo", "list[0]", "foo@bar", "a,b;c", "what?", "yes!"]
    )
    def test_valid_special_char_value_accepted(self, value):
        item = _filter_adapter.validate_python({"name": "field", "operator": "EQ", "value": value})
        assert item.value == value

    def test_numeric_values_accepted(self):
        item = _filter_adapter.validate_python({"name": "field", "operator": "GT", "value": 42})
        assert item.value == 42


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

    @pytest.mark.parametrize("operator", ["EQ", "NE", "GT", "GTE", "LT", "LTE", "TEXT_MATCH", "CONTAINS"])
    def test_scalar_operators_map_correctly(self, operator):
        engine = _make_engine()
        item = _filter_adapter.validate_python({"name": "field", "operator": operator, "value": "val"})
        result = engine._build_filter_object([item])
        assert result is not None
        assert result.filters[0].operator == _OPERATOR_MAP[operator]

    @pytest.mark.parametrize("operator", ["IN", "NIN", "ANY", "ALL"])
    def test_list_operators_map_correctly(self, operator):
        engine = _make_engine()
        item = _filter_adapter.validate_python({"name": "tags", "operator": operator, "value": ["a", "b"]})
        result = engine._build_filter_object([item])
        assert result.filters[0].operator == _OPERATOR_MAP[operator]

    def test_all_operators_covered_in_map(self):
        expected = {"EQ", "NE", "GT", "GTE", "LT", "LTE", "IN", "NIN", "ANY", "ALL", "TEXT_MATCH", "CONTAINS"}
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


class TestRetrieveTopK:
    def _mock_retriever(self, engine, nodes):
        retriever = Mock()
        retriever.retrieve.return_value = nodes
        index = Mock()
        index.as_retriever.return_value = retriever
        engine._index_cache = index
        return retriever

    def test_default_mode_when_hybrid_search_disabled(self):
        engine = _make_engine(hybrid_search=False)
        nodes = [_DummyNodeWithScore("a"), _DummyNodeWithScore("b")]
        retriever = self._mock_retriever(engine, nodes)

        result = engine.retrieve_top_k(query="test", top_k=5)

        kwargs = engine._index_cache.as_retriever.call_args.kwargs
        assert "vector_store_query_mode" not in kwargs
        assert "sparse_top_k" not in kwargs
        assert kwargs["similarity_top_k"] == 5
        retriever.retrieve.assert_called_once_with("test")
        assert result == nodes

    def test_hybrid_mode_requested_when_enabled(self):
        engine = _make_engine(hybrid_search=True)
        nodes = [_DummyNodeWithScore("a")]
        self._mock_retriever(engine, nodes)

        engine.retrieve_top_k(query="test", top_k=5)

        kwargs = engine._index_cache.as_retriever.call_args.kwargs
        assert kwargs["vector_store_query_mode"] == VectorStoreQueryMode.HYBRID
        assert kwargs["sparse_top_k"] == 5
        assert kwargs["similarity_top_k"] == 5

    def test_alpha_never_passed(self):
        for hybrid_search in (True, False):
            engine = _make_engine(hybrid_search=hybrid_search)
            self._mock_retriever(engine, [])

            engine.retrieve_top_k(query="test", top_k=5)

            kwargs = engine._index_cache.as_retriever.call_args.kwargs
            assert "alpha" not in kwargs

    def test_hybrid_results_trimmed_to_top_k(self):
        engine = _make_engine(hybrid_search=True)
        # Descending scores so a correct sort-then-trim keeps this exact prefix;
        # the assertion below also independently checks length and score order,
        # so this doesn't merely encode "positional slice" as expected behavior.
        nodes = [_DummyNodeWithScore(str(i), score=1.0 - i * 0.1) for i in range(10)]
        self._mock_retriever(engine, nodes)

        result = engine.retrieve_top_k(query="test", top_k=5)

        assert len(result) == 5
        assert result == nodes[:5]

    def test_hybrid_results_sorted_by_score_before_trim(self):
        # Simulates PGVectorStore's real behavior: dense results concatenated
        # before sparse results, unsorted. A lower-scoring dense hit ("dense_low")
        # would win a positional slice, but a higher-scoring sparse-only match
        # ("sparse_high") must win once results are sorted by score first.
        engine = _make_engine(hybrid_search=True)
        dense_low = _DummyNodeWithScore("dense_low", score=0.2)
        sparse_high = _DummyNodeWithScore("sparse_high", score=0.9)
        nodes = [dense_low, sparse_high]  # dense-first concatenation order
        self._mock_retriever(engine, nodes)

        result = engine.retrieve_top_k(query="test", top_k=1)

        assert result == [sparse_high]

    def test_default_mode_results_not_resorted(self):
        # DEFAULT mode already returns dense-sorted results from PGVectorStore;
        # retrieve_top_k must not reorder them.
        engine = _make_engine(hybrid_search=False)
        nodes = [_DummyNodeWithScore("a", score=0.9), _DummyNodeWithScore("b", score=0.1)]
        self._mock_retriever(engine, nodes)

        result = engine.retrieve_top_k(query="test", top_k=5)

        assert result == nodes

    def test_default_mode_also_trims_to_top_k(self):
        # The retriever's own similarity_top_k should already bound this, but
        # retrieve_top_k applies an unconditional nodes[:top_k] regardless of
        # mode — confirm that's a safe no-op-in-practice, not a behavior gap.
        engine = _make_engine(hybrid_search=False)
        nodes = [_DummyNodeWithScore(str(i), score=1.0 - i * 0.1) for i in range(10)]
        self._mock_retriever(engine, nodes)

        result = engine.retrieve_top_k(query="test", top_k=5)

        assert result == nodes[:5]

    def test_hybrid_sort_handles_none_score(self):
        # NodeWithScore.score can be None; the `n.score or 0.0` fallback must
        # not raise and must rank None-scored nodes below any real score.
        engine = _make_engine(hybrid_search=True)
        scored = _DummyNodeWithScore("scored", score=0.5)
        unscored = _DummyNodeWithScore("unscored", score=None)
        self._mock_retriever(engine, [unscored, scored])

        result = engine.retrieve_top_k(query="test", top_k=2)

        assert result == [scored, unscored]


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
