import logging

from langfuse import get_client
from openinference.instrumentation.llama_index import LlamaIndexInstrumentor

logger = logging.getLogger(__name__)

_instrumentor = LlamaIndexInstrumentor()
langfuse_client = get_client()


def is_enabled() -> bool:
    """Return True if observability is enabled."""
    return _instrumentor.is_instrumented_by_opentelemetry


def setup_observability(config: dict) -> None:
    """Configure Langfuse observability for LlamaIndex if enabled.

    Uses OpenInference instrumentation to capture LlamaIndex spans and
    export them to Langfuse via OpenTelemetry. Credentials are read from
    environment variables (LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY,
    LANGFUSE_BASE_URL) set via config.yaml interpolation.
    """
    tracing_enabled = config.get("tracing_enabled")
    if str(tracing_enabled).lower() in ("false", "0", "none", ""):
        logger.info("Observability disabled")
        return

    _instrumentor.instrument()

    try:
        if langfuse_client.auth_check():
            logger.info("Langfuse client is authenticated and ready!")
        else:
            logger.warning("Langfuse authentication failed. Please check your credentials and host.")
    except Exception:
        logger.warning("Langfuse auth check failed. Langfuse may not be reachable.")
