import os
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.llms.openrouter import OpenRouter
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from utils.config import settings


os.environ["CUDA_VISIBLE_DEVICES"] = settings.env.CUDA_VISIBLE_DEVICES
os.environ["ORT_DISABLE_GPU"] = str(settings.env.ORT_DISABLE_GPU)
os.environ["ORT_DYLD_DISABLE_GPU"] = str(settings.env.ORT_DYLD_DISABLE_GPU)

embeddings_provider = settings.EMBEDDING.get("provider")
llm_provider = settings.LLM.get("provider")


# Initialize embedding model
if embeddings_provider == "local":
    embed_model = HuggingFaceEmbedding(
        model_name = settings.EMBEDDING["model_config"],
        trust_remote_code=True,
        device="cpu"
    )
else:
    embed_model = OpenAIEmbedding(
        api_key=settings.LLM["api_key"],
        api_base=settings.LLM["base_url"],
        model_name=settings.EMBEDDING["model_config"]
    )

# Initialize LLM
llm = None
if llm_provider in ("openai", "openrouter"):
    llm = OpenRouter(
        api_key=settings.LLM.get("api_key"),
        model=settings.LLM.get("llm_model"),
        api_base=settings.LLM["base_url"]
    )

__all__ = ["embed_model", "llm"]
