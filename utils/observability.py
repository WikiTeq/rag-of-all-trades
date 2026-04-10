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

    os.environ["LANGFUSE_PUBLIC_KEY"] = config["public_key"]
    os.environ["LANGFUSE_SECRET_KEY"] = config["secret_key"]
    os.environ["LANGFUSE_HOST"] = config["host"]

    set_global_handler(
        "langfuse",
        public_key=config["public_key"],
        secret_key=config["secret_key"],
        host=config["host"],
    )
    # Reset the callback manager so that the global handler set above is picked
    # up by any LlamaIndex component that was already initialized before this
    # function was called (e.g. LLM/embed model set in utils/llm_embedding.py).
    Settings.callback_manager = CallbackManager()

    logger.info("Langfuse observability enabled, host=%s", config["host"])
