from unittest.mock import Mock, patch

import pytest
from llama_index.core.schema import NodeWithScore, TextNode
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


def _real_node_with_score(text: str, score: float) -> NodeWithScore:
    """A real NodeWithScore/TextNode pair, for tests exercising the actual
    QueryFusionRetriever fusion path — unlike the plain-mocked single-
    retriever path, QueryFusionRetriever.retrieve() validates its nodes as
    real NodeWithScore instances (via a pydantic event payload), so the
    lightweight _DummyNodeWithScore stand-in used elsewhere in this file
    doesn't satisfy it.
    """
    return NodeWithScore(node=TextNode(text=text, id_=text), score=score)


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

    def _mock_hybrid_retrievers(self, engine, dense_nodes, sparse_nodes):
        """Mock the two legs QueryFusionRetriever fuses for hybrid_search=True:
        as_retriever is called twice (DEFAULT then SPARSE), each returning its
        own retriever mock. Real RRF fusion runs on top — not mocked — so
        these tests prove actual fusion behavior, not just that fusion was
        invoked.
        """
        dense_retriever = Mock()
        dense_retriever.retrieve.return_value = dense_nodes
        sparse_retriever = Mock()
        sparse_retriever.retrieve.return_value = sparse_nodes

        index = Mock()

        def as_retriever(**kwargs):
            if kwargs.get("vector_store_query_mode") == VectorStoreQueryMode.SPARSE:
                return sparse_retriever
            return dense_retriever

        index.as_retriever.side_effect = as_retriever
        engine._index_cache = index
        return dense_retriever, sparse_retriever

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

    def test_hybrid_mode_builds_dense_and_sparse_retrievers(self):
        # Confirms the two as_retriever calls that back the fusion, per
        # PR98-fixes.md "Commit 1": one DEFAULT (dense), one SPARSE, each
        # windowed to _fusion_window(top_k) — not top_k itself — so RRF has
        # enough candidates from each leg to fuse from.
        engine = _make_engine(hybrid_search=True)
        self._mock_hybrid_retrievers(engine, dense_nodes=[], sparse_nodes=[])

        engine.retrieve_top_k(query="test", top_k=5)

        calls = engine._index_cache.as_retriever.call_args_list
        assert len(calls) == 2
        dense_kwargs = next(
            c.kwargs for c in calls if c.kwargs.get("vector_store_query_mode") != VectorStoreQueryMode.SPARSE
        )
        sparse_kwargs = next(
            c.kwargs for c in calls if c.kwargs.get("vector_store_query_mode") == VectorStoreQueryMode.SPARSE
        )

        assert dense_kwargs["vector_store_query_mode"] == VectorStoreQueryMode.DEFAULT
        assert dense_kwargs["similarity_top_k"] == 60  # _fusion_window(5) == max(60, 4*5)
        assert sparse_kwargs["similarity_top_k"] == 60
        assert sparse_kwargs["sparse_top_k"] == 60

    def test_hybrid_fusion_window_scales_with_top_k(self):
        engine = _make_engine(hybrid_search=True)
        self._mock_hybrid_retrievers(engine, dense_nodes=[], sparse_nodes=[])

        engine.retrieve_top_k(query="test", top_k=20)

        calls = engine._index_cache.as_retriever.call_args_list
        for c in calls:
            # _fusion_window(20) == max(60, 4*20) == 80
            assert c.kwargs["similarity_top_k"] == 80

    def test_alpha_never_passed(self):
        for hybrid_search in (True, False):
            engine = _make_engine(hybrid_search=hybrid_search)
            if hybrid_search:
                self._mock_hybrid_retrievers(engine, dense_nodes=[], sparse_nodes=[])
            else:
                self._mock_retriever(engine, [])

            engine.retrieve_top_k(query="test", top_k=5)

            for c in engine._index_cache.as_retriever.call_args_list:
                assert "alpha" not in c.kwargs

    def test_hybrid_surfaces_sparse_only_match_despite_incomparable_raw_scores(self):
        # The exact bug pastakhov's finding describes: a full page of dense
        # hits with realistic dense scores (~0.7-0.9) plus one sparse-only
        # keyword match with a realistic, much smaller raw ts_rank (~0.05).
        # The old raw-score sort would drop the sparse-only match entirely
        # (0.05 < every dense score). RRF ranks each leg by its own order
        # instead, so a top-ranked sparse-only hit can still win a final slot
        # even though its raw score looks tiny next to dense's.
        engine = _make_engine(hybrid_search=True)
        dense_nodes = [_real_node_with_score(f"dense-{i}", score=0.9 - i * 0.02) for i in range(5)]
        sparse_only = _real_node_with_score("sparse-only-keyword-hit", score=0.05)
        self._mock_hybrid_retrievers(engine, dense_nodes=dense_nodes, sparse_nodes=[sparse_only])

        result = engine.retrieve_top_k(query="test", top_k=3)

        result_texts = {n.node.get_text() for n in result}
        assert "sparse-only-keyword-hit" in result_texts, (
            "RRF must surface a top-ranked sparse-only match even though its raw "
            "ts_rank score is far smaller than every dense score"
        )

    def test_hybrid_dedups_a_node_found_by_both_legs(self):
        # The same underlying chunk can rank in both the dense and sparse
        # legs (e.g. a semantically and lexically relevant match). RRF must
        # fuse it into one result, not return it twice.
        engine = _make_engine(hybrid_search=True)
        shared = _real_node_with_score("shared-node", score=0.8)
        dense_nodes = [shared, _real_node_with_score("dense-only", score=0.7)]
        sparse_nodes = [_real_node_with_score("shared-node", score=0.3)]
        self._mock_hybrid_retrievers(engine, dense_nodes=dense_nodes, sparse_nodes=sparse_nodes)

        result = engine.retrieve_top_k(query="test", top_k=5)

        result_texts = [n.node.get_text() for n in result]
        assert result_texts.count("shared-node") == 1

    def test_hybrid_result_count_respects_top_k(self):
        engine = _make_engine(hybrid_search=True)
        dense_nodes = [_real_node_with_score(f"dense-{i}", score=1.0 - i * 0.05) for i in range(10)]
        sparse_nodes = [_real_node_with_score(f"sparse-{i}", score=1.0 - i * 0.05) for i in range(10)]
        self._mock_hybrid_retrievers(engine, dense_nodes=dense_nodes, sparse_nodes=sparse_nodes)

        result = engine.retrieve_top_k(query="test", top_k=4)

        assert len(result) == 4

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
