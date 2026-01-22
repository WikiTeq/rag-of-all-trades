import gc
from utils.llm_embedding import embed_model
from utils.config import settings

class VectorStoreManager:
    def __init__(self):
        self._initialized = False
        self.vector_store = None
        self.storage_context = None

    def _init_if_needed(self):
        if self._initialized:
            return
        from llama_index.vector_stores.postgres import PGVectorStore
        from llama_index.core import StorageContext

        postgres = settings.POSTGRES
        embedding = settings.EMBEDDING

        self.vector_store = PGVectorStore.from_params(
            database=postgres["database"],
            host=postgres["host"],
            password=postgres["password"],
            port=postgres["port"],
            user=postgres["user"],
            table_name=postgres["table_name"],
            embed_dim=embedding["dim"],
            hybrid_search=postgres.get("hybrid_search", True),
            hnsw_kwargs={
                "hnsw_m": postgres.get("hnsw_m", 16),
                "hnsw_ef_construction": postgres.get("hnsw_ef_construction", 64),
                "hnsw_ef_search": postgres.get("hnsw_ef_search", 40),
                "hnsw_dist_method": postgres.get("hnsw_dist_method", "vector_cosine_ops"),
            },
        )
        self.storage_context = StorageContext.from_defaults(vector_store=self.vector_store)
        self._initialized = True

    def insert_documents(self, docs: list):
        self._init_if_needed()
        from llama_index.core import VectorStoreIndex
        from llama_index.core.ingestion import IngestionPipeline
        from llama_index.core.node_parser import SentenceSplitter

        vector_store = settings.POSTGRES
        
        splitter = SentenceSplitter(
            chunk_size=vector_store["chunk_size"],
            chunk_overlap=vector_store["chunk_overlap"],
        )

        pipeline = IngestionPipeline(
            transformations=[
                splitter,
                embed_model,
            ]
        )

        nodes = pipeline.run(documents=docs)

        VectorStoreIndex(nodes=nodes, embed_model=embed_model, storage_context=self.storage_context)

        try:
            del docs
        except Exception:
            pass
        gc.collect()