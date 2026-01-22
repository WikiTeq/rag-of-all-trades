from typing import List, Optional, Dict, Any
from llama_index.core import VectorStoreIndex
from llama_index.core.vector_stores.types import VectorStore
from llama_index.core.schema import NodeWithScore

from llama_index.core.vector_stores.types import (
    MetadataFilter,
    MetadataFilters,
    FilterOperator,
    FilterCondition,
)

from llama_index.core import Settings
from utils.llm_embedding import embed_model, llm

Settings.llm = llm
Settings.embed_model = embed_model


class RAGQueryEngine:
    def __init__(self, vector_store: VectorStore):
        self.vector_store = vector_store
        self._index_cache = None  # Cache the index to avoid recreating it

    #Convert metadata dict → LlamaIndex MetadataFilters
    def _build_filter_object(
        self,
        metadata: Optional[Dict[str, Any]]
    ) -> Optional[MetadataFilters]:

        if not metadata:
            return None

        filters: List[MetadataFilter] = []

        for key, value in metadata.items():
            if isinstance(value, list):
                # multiple values -> IN operator
                filters.append(
                    MetadataFilter(
                        key=key,
                        value=value,
                        operator=FilterOperator.IN
                    )
                )
            else:
                # single value -> EQ
                filters.append(
                    MetadataFilter(
                        key=key,
                        value=value,
                        operator=FilterOperator.EQ
                    )
                )

        return MetadataFilters(
            filters=filters,
            condition=FilterCondition.AND
        )
    
    # Create cleaned reference objects
    @staticmethod
    def build_references(nodes: List[NodeWithScore]):
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
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[NodeWithScore]:

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
