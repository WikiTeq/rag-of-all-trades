import logging

from langfuse.llama_index import LlamaIndexInstrumentor

logger = logging.getLogger(__name__)


def setup_observability(config: dict) -> None:
    """Configure Langfuse observability for LlamaIndex if enabled.

    Uses the LlamaIndex instrumentation API (non-deprecated) to register
    Langfuse as a global instrumentor. Credentials are read from environment
    variables that are already set via config.yaml interpolation.
    """
    if not config.get("enabled"):
        logger.info("Observability disabled")
        return

    public_key = config.get("public_key", "")
    secret_key = config.get("secret_key", "")
    host = config.get("host", "")
    if not public_key or not secret_key or not host:
        raise ValueError("Observability enabled but Langfuse credentials/host are missing")

    instrumentor = LlamaIndexInstrumentor(
        public_key=public_key,
        secret_key=secret_key,
        host=host,
    )
    instrumentor.start()

    logger.info("Langfuse observability enabled, host=%s", host)
