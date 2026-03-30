import os
from pathlib import Path

import yaml
from cryptography.fernet import Fernet
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"
YAML_PATH = BASE_DIR / "config.yaml"


class EnvSettings(BaseSettings):
    """Pydantic settings model loaded from .env and environment variables."""

    REDIS_URL: str

    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int
    DATABASE_URL: str

    OPENROUTER_API_KEY: str
    OPENROUTER_API_BASE: str
    MCP_ENABLE: bool = False
    MCP_API_KEY: str = ""

    @field_validator("MCP_API_KEY", mode="before")
    @classmethod
    def strip_mcp_api_key(cls, v: str) -> str:
        if not isinstance(v, str):
            raise ValueError("MCP_API_KEY must be a string")
        return v.strip()

    CELERY_CONCURRENCY: int = 2  # Default fallback
    MAX_TASK_CHILD: int = 50
    MAX_MEMORY_PER_CHILD: int = 300000

    CUDA_VISIBLE_DEVICES: str = ""
    ORT_DISABLE_GPU: int = 1
    ORT_DYLD_DISABLE_GPU: int = 1

    CORS_ORIGINS: list[str] = []

    ENABLE_RATE_LIMIT: bool = False
    CHUNK_RATE_LIMIT: str = "30/minute"
    REPHRASE_RATE_LIMIT: str = "30/minute"

    # generate with:
    # python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    CONNECTOR_ENCRYPTION_KEY: str = ""

    @field_validator("CONNECTOR_ENCRYPTION_KEY", mode="after")
    @classmethod
    def validate_encryption_key(cls, v):
        """Raise ValueError if CONNECTOR_ENCRYPTION_KEY is set but not a valid Fernet key."""
        if not v:
            return v
        try:
            Fernet(v.encode() if isinstance(v, str) else v)
        except Exception as e:
            raise ValueError(f"CONNECTOR_ENCRYPTION_KEY is not a valid Fernet key: {e}") from e
        return v

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def parse_cors_origins(cls, v):
        """Parse CORS_ORIGINS from a comma-separated string or list."""
        if isinstance(v, str):
            return [x.strip() for x in v.split(",") if x.strip()]
        return v

    model_config = SettingsConfigDict(env_file=ENV_PATH, case_sensitive=True, extra="allow")


def load_yaml_with_env(path):
    """Load a YAML file with ${ENV_VAR} interpolation from os.environ."""
    with open(path) as f:
        raw_yaml = f.read()
    # interpolate ${VAR} with os.environ
    for key, value in os.environ.items():
        raw_yaml = raw_yaml.replace(f"${{{key}}}", value)
    return yaml.safe_load(raw_yaml)


class Settings:
    """Unified config object combining .env settings and config.yaml values."""

    def __init__(self):
        """Load EnvSettings and config.yaml on construction."""
        self.env = EnvSettings()
        self.yaml = load_yaml_with_env(YAML_PATH)

    @property
    def POSTGRES(self):
        """Return PostgreSQL / pgvector connection and table settings."""
        vector_store = self.yaml.get("vector_store", {})
        hnsw = vector_store.get("hnsw", {})
        return {
            "user": self.env.POSTGRES_USER,
            "password": self.env.POSTGRES_PASSWORD,
            "host": self.env.POSTGRES_HOST,
            "port": self.env.POSTGRES_PORT,
            "database": self.env.POSTGRES_DB,
            "table_name": vector_store.get("table_name", "embeddings"),
            "hybrid_search": vector_store.get("hybrid_search", True),
            "hnsw_m": hnsw.get("hnsw_m", 16),
            "hnsw_ef_construction": hnsw.get("hnsw_ef_construction", 64),
            "hnsw_ef_search": hnsw.get("hnsw_ef_search", 40),
            "hnsw_dist_method": hnsw.get("hnsw_dist_method", "vector_cosine_ops"),
            "chunk_size": vector_store.get("chunk_size", 512),
            "chunk_overlap": vector_store.get("chunk_overlap", 50),
        }

    @property
    def EMBEDDING(self):
        """Return embedding model settings from config.yaml."""
        return {
            "provider": self.yaml.get("embedding", {}).get("provider"),
            "model_config": self.yaml.get("embedding", {}).get("model_config"),
            "dim": self.yaml.get("embedding", {}).get("embedding_dim"),
        }

    @property
    def LLM(self):
        """Return LLM inference settings from .env and config.yaml."""
        return {
            "api_key": self.env.OPENROUTER_API_KEY,
            "base_url": self.env.OPENROUTER_API_BASE,
            "provider": self.yaml.get("inference", {}).get("provider"),
            "llm_model": self.yaml.get("inference", {}).get("model_config"),
        }


settings = Settings()
