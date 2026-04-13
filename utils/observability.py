import logging

from langfuse.llama_index import LlamaIndexInstrumentor

logger = logging.getLogger(__name__)

_instrumentor: LlamaIndexInstrumentor | None = None


def get_instrumentor() -> LlamaIndexInstrumentor | None:
    """Return the active LlamaIndexInstrumentor, or None if observability is disabled."""
    return _instrumentor


def setup_observability(config: dict) -> None:
    """Configure Langfuse observability for LlamaIndex if enabled.

    Uses the LlamaIndex instrumentation API (non-deprecated) to register
    Langfuse as a global instrumentor. Credentials are read from environment
    variables that are already set via config.yaml interpolation.
    """
    global _instrumentor

    if not config.get("enabled"):
        logger.info("Observability disabled")
        return

    _instrumentor = LlamaIndexInstrumentor()
    _instrumentor.start()

    logger.info("Langfuse observability enabled")
