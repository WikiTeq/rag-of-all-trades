from llama_index.core import Settings, VectorStoreIndex
from llama_index.core.retrievers import QueryFusionRetriever
from llama_index.core.schema import NodeWithScore
from llama_index.core.vector_stores.types import (
    FilterCondition,
    FilterOperator,
    MetadataFilter,
    MetadataFilters,
    VectorStore,
    VectorStoreQueryMode,
)

from api.v1.chunk_retrieval.schema import MetadataFilterItem
from utils.llm_embedding import embed_model, llm

Settings.llm = llm
Settings.embed_model = embed_model

# Reciprocal rank fusion (RRF, k=60 per llama_index's own QueryFusionRetriever
# implementation) needs each leg's window to comfortably exceed the final
# top_k it's fusing down to, or a result ranked just past a small top_k in
# one leg can never surface via its combined rank. No source-level guidance
# exists for an exact multiplier; 4x (floored at 60, RRF's own constant) is
# a reasonable margin without unbounded DB cost growth for large top_k.
_MIN_FUSION_WINDOW = 60
_FUSION_WINDOW_MULTIPLIER = 4


def _fusion_window(top_k: int) -> int:
    return max(_MIN_FUSION_WINDOW, _FUSION_WINDOW_MULTIPLIER * top_k)


_OPERATOR_MAP: dict[str, FilterOperator] = {
    "EQ": FilterOperator.EQ,
    "NE": FilterOperator.NE,
    "GT": FilterOperator.GT,
    "GTE": FilterOperator.GTE,
    "LT": FilterOperator.LT,
    "LTE": FilterOperator.LTE,
    "IN": FilterOperator.IN,
    "NIN": FilterOperator.NIN,
    "ANY": FilterOperator.ANY,
    "ALL": FilterOperator.ALL,
    "TEXT_MATCH": FilterOperator.TEXT_MATCH,
    "CONTAINS": FilterOperator.CONTAINS,
}


class RAGQueryEngine:
    def __init__(self, vector_store: VectorStore, hybrid_search: bool = False):
        self.vector_store = vector_store
        self.hybrid_search = hybrid_search
        self._index_cache = None  # Cache the index to avoid recreating it

    def _build_filter_object(self, metadata: list[MetadataFilterItem] | None) -> MetadataFilters | None:
        if not metadata:
            return None

        filters: list[MetadataFilter] = [
            MetadataFilter(key=item.name, value=item.value, operator=_OPERATOR_MAP[item.operator]) for item in metadata
        ]

        return MetadataFilters(filters=filters, condition=FilterCondition.AND)

    # Create cleaned reference objects
    @staticmethod
    def build_references(nodes: list[NodeWithScore]):
        refs = []
        for n in nodes:
            md = n.node.metadata or {}
            refs.append(
                {
                    "source_name": md.get("source_name"),
                    "source_type": md.get("source_type"),
                    "url": md.get("source_url") or md.get("url") or md.get("path"),
                    "score": n.score,
                    "title": md.get("title") or md.get("file_name"),
                    "text": n.node.get_content(),
                    "extras": {
                        k: v
                        for k, v in md.items()
                        if k
                        not in {
                            "source_name",
                            "source_type",
                            "source_url",
                            "title",
                            "file_name",
                        }
                    },
                }
            )
        return refs

    # Retrieve top K with optional metadata filter
    def retrieve_top_k(
        self,
        query: str,
        top_k: int = 5,
        metadata: list[MetadataFilterItem] | None = None,
    ) -> list[NodeWithScore]:
        # Use cached index to avoid recreating on every query
        if self._index_cache is None:
            self._index_cache = VectorStoreIndex.from_vector_store(self.vector_store)

        # Convert metadata dict → MetadataFilters
        metadata_filters = self._build_filter_object(metadata)

        if not self.hybrid_search:
            retriever = self._index_cache.as_retriever(
                similarity_top_k=top_k,
                filters=metadata_filters,
            )
            nodes = retriever.retrieve(query)
            return nodes[:top_k]

        # Hybrid search: fuse a dense-similarity retriever and a sparse
        # full-text (ts_rank) retriever via reciprocal rank fusion (RRF),
        # instead of PGVectorStore's own HYBRID mode. PGVectorStore's HYBRID
        # concatenates dense + sparse rows and dedups by node id, but never
        # ranks the merged list — the two scores it returns (dense:
        # 1 - cosine_distance, sparse: raw Postgres ts_rank) are on
        # incompatible scales, so sorting or trimming that merged list by
        # raw score just picks whichever metric happens to produce bigger
        # numbers, not the more relevant result
        # (https://github.com/WikiTeq/rag-of-all-trades/pull/98#discussion_r3978297351).
        # RRF instead ranks each leg independently by its own metric and
        # fuses by rank (1 / (rank + 60)), which is scale-agnostic.
        window = _fusion_window(top_k)
        dense_retriever = self._index_cache.as_retriever(
            vector_store_query_mode=VectorStoreQueryMode.DEFAULT,
            similarity_top_k=window,
            filters=metadata_filters,
        )
        sparse_retriever = self._index_cache.as_retriever(
            vector_store_query_mode=VectorStoreQueryMode.SPARSE,
            # PGVectorStore's SPARSE mode uses sparse_top_k, falling back to
            # similarity_top_k only if sparse_top_k is unset — set both
            # explicitly to the same window so the effective limit is
            # unambiguous rather than relying on that fallback.
            similarity_top_k=window,
            sparse_top_k=window,
            filters=metadata_filters,
        )
        fusion_retriever = QueryFusionRetriever(
            retrievers=[dense_retriever, sparse_retriever],
            mode="reciprocal_rerank",
            num_queries=1,  # skip LLM-based query rewriting — this endpoint is LLM-free
            use_async=False,  # sequential, predictable; matches the non-fused path's behavior
            similarity_top_k=top_k,
        )
        return fusion_retriever.retrieve(query)
