import os
from pathlib import Path
import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator
from typing import List

BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"
YAML_PATH = BASE_DIR / "config.yaml"


class EnvSettings(BaseSettings):
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

    CELERY_CONCURRENCY: int = 2  #Default fallback
    MAX_TASK_CHILD: int = 50
    MAX_MEMORY_PER_CHILD: int = 300000

    CUDA_VISIBLE_DEVICES: str = ""
    ORT_DISABLE_GPU: int = 1
    ORT_DYLD_DISABLE_GPU: int = 1

    CORS_ORIGINS: List[str] = []

    ENABLE_RATE_LIMIT: bool = True
    CHUNK_RATE_LIMIT: str = "30/minute"
    REPHRASE_RATE_LIMIT: str = "20/minute"

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def parse_cors_origins(cls, v):
        if isinstance(v, str):
            return [x.strip() for x in v.split(",") if x.strip()]
        return v

    model_config = SettingsConfigDict(
        env_file=ENV_PATH,
        case_sensitive=True,
        extra="allow"
    )


def load_yaml_with_env(path):
    with open(path, "r") as f:
        raw_yaml = f.read()
    # interpolate ${VAR} with os.environ
    for key, value in os.environ.items():
        raw_yaml = raw_yaml.replace(f"${{{key}}}", value)
    return yaml.safe_load(raw_yaml)


class Settings:
    def __init__(self):
        self.env = EnvSettings()
        self.yaml = load_yaml_with_env(YAML_PATH)

    @property
    def POSTGRES(self):
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
        return {
            "provider": self.yaml.get("embedding", {}).get("provider"),
            "model_config": self.yaml.get("embedding", {}).get("model_config"),
            "dim": self.yaml.get("embedding", {}).get("embedding_dim")
        }

    @property
    def SOURCES(self):
        """Generic loader for all sources (S3, future types)"""
        raw_sources = self.yaml.get("sources", [])
        sources = []

        for source in raw_sources:
            src_type = source.get("type")
            name = source.get("name", "unknown_source")
            config = source.get("config", {})

            # Handle buckets as list
            buckets = config.get("buckets", [])
            if isinstance(buckets, str):
                buckets = [b.strip() for b in buckets.split(",") if b.strip()]

            schedules = config.get("schedules", [])
            if isinstance(schedules, str):
                schedules = [s.strip() for s in schedules.split(",") if s.strip()]

            if buckets:
                for i, bucket in enumerate(buckets):
                    try:
                        schedule_seconds = int(schedules[i]) if i < len(schedules) else 3600
                    except ValueError:
                        schedule_seconds = 3600

                    # Merge common chunk settings from embedding
                    sources.append({
                        "type": src_type,
                        "name": f"{name}_{bucket}",
                        "config": {**config, "buckets": [bucket],
                                "bucket_override": bucket},
                        "schedule": schedule_seconds
                    })
            else:
                schedule_seconds = int(schedules[0]) if schedules else 3600
                sources.append({
                    "type": src_type,
                    "name": name,
                    "config": config,
                    "schedule": schedule_seconds
                })
        return sources

    @property
    def LLM(self):
        return {
            "api_key": self.env.OPENROUTER_API_KEY,
            "base_url": self.env.OPENROUTER_API_BASE,
            "provider": self.yaml.get("inference", {}).get("provider"),
            "llm_model": self.yaml.get("inference", {}).get("model_config")
        }

settings = Settings()
