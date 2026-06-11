from llama_index.core import Settings, VectorStoreIndex
from llama_index.core.schema import NodeWithScore
from llama_index.core.vector_stores.types import (
    FilterCondition,
    FilterOperator,
    MetadataFilter,
    MetadataFilters,
    VectorStore,
)

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
    "TEXT_MATCH": FilterOperator.TEXT_MATCH,
}


class RAGQueryEngine:
    def __init__(self, vector_store: VectorStore):
        self.vector_store = vector_store
        self._index_cache = None  # Cache the index to avoid recreating it

    def _build_filter_object(self, metadata: list | None) -> MetadataFilters | None:
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
        metadata: list | None = None,
    ) -> list[NodeWithScore]:
        # Use cached index to avoid recreating on every query
        if self._index_cache is None:
            self._index_cache = VectorStoreIndex.from_vector_store(self.vector_store)

        # Convert metadata dict → MetadataFilters
        metadata_filters = self._build_filter_object(metadata)

        retriever = self._index_cache.as_retriever(
            similarity_top_k=top_k,
            filters=metadata_filters,
        )

        nodes = retriever.retrieve(query)
        return nodes
