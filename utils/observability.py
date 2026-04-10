import logging
import os

from llama_index.core import Settings, set_global_handler
from llama_index.core.callbacks import CallbackManager

logger = logging.getLogger(__name__)


def setup_observability(config: dict) -> None:
    """Configure Langfuse observability for LlamaIndex if enabled.

    Sets the global LlamaIndex callback handler using LlamaIndex's native
    Langfuse integration. Also resets Settings.callback_manager so that the
    Langfuse handler is picked up even if Settings was already accessed before
    this function was called (e.g. during LLM/embed model initialization).

    Reads credentials from the config dict and sets the required environment
    variables before activating the handler, so that the caller does not need
    to pre-set them in the environment.
    """
    if not config.get("enabled"):
        logger.info("Observability disabled")
        return

    public_key = config.get("public_key", "")
    secret_key = config.get("secret_key", "")
    host = config.get("host", "")
    if not public_key or not secret_key or not host:
        raise ValueError("Observability enabled but Langfuse credentials/host are missing")

    os.environ["LANGFUSE_PUBLIC_KEY"] = public_key
    os.environ["LANGFUSE_SECRET_KEY"] = secret_key
    os.environ["LANGFUSE_HOST"] = host

    set_global_handler(
        "langfuse",
        public_key=public_key,
        secret_key=secret_key,
        host=host,
    )
    # Reset the callback manager so that the global handler set above is picked
    # up by any LlamaIndex component that was already initialized before this
    # function was called (e.g. LLM/embed model set in utils/llm_embedding.py).
    Settings.callback_manager = CallbackManager()

    logger.info("Langfuse observability enabled, host=%s", host)
