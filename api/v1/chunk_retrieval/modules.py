from llama_index.core import Settings, VectorStoreIndex
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

        retriever_kwargs = {
            "similarity_top_k": top_k,
            "filters": metadata_filters,
        }
        if self.hybrid_search:
            # PGVectorStore's hybrid mode unions dense + sparse results and dedups,
            # but does not support `alpha` (it logs a warning and ignores it).
            retriever_kwargs["vector_store_query_mode"] = VectorStoreQueryMode.HYBRID
            retriever_kwargs["sparse_top_k"] = top_k

        retriever = self._index_cache.as_retriever(**retriever_kwargs)

        nodes = retriever.retrieve(query)

        if self.hybrid_search:
            # PGVectorStore concatenates dense results before sparse results and
            # dedups by node id, without sorting the merged list or trimming it
            # back down to top_k. Left as-is, a plain nodes[:top_k] slice would
            # keep only dense hits (they come first) and silently drop every
            # sparse-only match. Sort by score before trimming so both dense and
            # sparse hits compete for the final top_k slots. Dense scores (cosine
            # similarity) and sparse scores (Postgres ts_rank) are on different
            # scales, so this is an approximation, not true fused ranking — but
            # it is strictly better than keeping whichever list happens to be
            # concatenated first.
            nodes = sorted(nodes, key=lambda n: n.score or 0.0, reverse=True)

        return nodes[:top_k]
