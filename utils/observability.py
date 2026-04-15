import logging

from openinference.instrumentation.llama_index import LlamaIndexInstrumentor

logger = logging.getLogger(__name__)

_instrumentor = LlamaIndexInstrumentor()


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
    if not config.get("enabled"):
        logger.info("Observability disabled")
        return

    _instrumentor.instrument()

    logger.info("Langfuse observability enabled")
