import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import Any, MutableMapping
from starlette.types import ASGIApp, Receive, Scope, Send
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from llama_index.vector_stores.postgres import PGVectorStore
from api.v1 import api_v1_router
from api.v1.chunk_retrieval.modules import RAGQueryEngine
from api.mcp_server import create_mcp_server
from utils.config import settings
from celery_app import celery_app

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)


class _NormalizeMountedRootPath:
    """Normalize mounted root path to avoid /mcp -> /mcp/ redirect."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(
        self,
        scope: MutableMapping[str, Any] | Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        if scope.get("type") == "http" and scope.get("path") == "":
            scope["path"] = "/"
            scope["raw_path"] = b"/"
        await self.app(scope, receive, send)


class _MCPNoSlashAlias:
    """Serve /mcp without framework redirect to /mcp/."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(
        self,
        scope: MutableMapping[str, Any] | Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        if scope.get("type") == "http" and scope.get("path") == "/mcp":
            scope = dict(scope)
            scope["path"] = "/"
            scope["raw_path"] = b"/"
        await self.app(scope, receive, send)


def validate_configuration():
    """Validate critical configuration on startup."""
    errors = []

    # Validate embedding configuration
    if not settings.EMBEDDING.get("provider"):
        errors.append("Embedding provider not configured")
    if not settings.EMBEDDING.get("model_config"):
        errors.append("Embedding model not configured")

    # Validate embedding dimension (must be positive integer)
    dim = settings.EMBEDDING.get("dim")
    if not dim:
        errors.append("Embedding dimension not configured")
    elif not isinstance(dim, int):
        errors.append(f"Embedding dimension must be an integer, got: {type(dim).__name__}")
    elif dim <= 0:
        errors.append(f"Embedding dimension must be positive, got: {dim}")

    # Validate PostgreSQL configuration
    postgres = settings.POSTGRES
    required_postgres = ["user", "password", "host", "port", "database"]
    for key in required_postgres:
        if not postgres.get(key):
            errors.append(f"PostgreSQL {key} not configured")

    # Validate sources
    if not settings.SOURCES:
        logger.warning("No ingestion sources configured - Celery tasks will not be registered")

    # Validate LLM for rephrase endpoint (optional but warn)
    if settings.LLM.get("provider") not in ("openai", "openrouter", None):
        logger.warning(f"Unknown LLM provider: {settings.LLM.get('provider')}. Rephrase endpoint may not work.")

    # Validate MCP API key
    if not settings.env.MCP_API_KEY:
        errors.append("MCP_API_KEY not configured")

    if errors:
        error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info("Configuration validation passed")

@asynccontextmanager
async def app_lifespan(app: FastAPI):
    """Lifespan event handler for startup and shutdown."""

    #Startup - Validate configuration first
    validate_configuration()

    postgres = settings.POSTGRES
    embedding = settings.EMBEDDING

    app.state.vector_store = PGVectorStore.from_params(
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

    # Initialize RAG engine
    app.state.rag_engine = RAGQueryEngine(app.state.vector_store)
    logger.info(f"Vector store and RAG engine initialized")

    # Yield to FastAPI runtime
    yield

    # Shutdown
    logger.info(f"Cleaning up resources... done.")


# Create app instance
app = FastAPI(
    title="Rag Of All Trades",
    description="RAG Service",
    version="0.1.0",
)

# Configure MCP server and attach combined lifespan
mcp_server = create_mcp_server(app=app, api_key=settings.env.MCP_API_KEY)
mcp_http_app = mcp_server.http_app(path="/", stateless_http=True)

# IMPORTANT: Starlette's app.mount() does NOT invoke the mounted sub-app's
# lifespan. FastMCP's StreamableHTTPSessionManager requires its lifespan to
# run (it initializes the task group). We nest it inside the main app lifespan
# here so both are managed together. If this combined_lifespan is removed or
# refactored, MCP will fail at runtime on the first request with:
#   "Task group is not initialized. Make sure to use run()."
@asynccontextmanager
async def combined_lifespan(app_instance: FastAPI):
    async with app_lifespan(app_instance):
        mcp_lifespan = getattr(mcp_http_app, "lifespan", None)
        if mcp_lifespan is None:
            yield
        else:
            async with mcp_lifespan(app_instance):
                yield

app.router.lifespan_context = combined_lifespan

# Add rate limiter to app state
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

#Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.env.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_route(
    "/mcp",
    _MCPNoSlashAlias(mcp_http_app),
    methods=["GET", "POST", "DELETE"],
    include_in_schema=False,
)
app.mount("/mcp", _NormalizeMountedRootPath(mcp_http_app))

@app.get("/health")
def health_check():
    """Combined health check for FastAPI, vector store, and Celery."""
    vector_store_loaded = hasattr(app.state, "vector_store")
    celery_ok = False
    try:
        response = celery_app.control.ping(timeout=1.0)
        celery_ok = bool(response)
    except Exception as e:
        celery_ok = False

    return {
        "status": "ok",
        "vector_store_loaded": vector_store_loaded,
        "celery_healthy": celery_ok,
    }

@app.get("/")
async def read_root():
    return {"message": "Welcome to RAG Service Backend"}

app.include_router(api_v1_router)
