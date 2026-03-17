from typing import Any

from pydantic import BaseModel, field_validator


class QueryRequest(BaseModel):
    query: str
    top_k: int = 20
    metadata_filters: dict[str, Any] | None = None

    # Validator for top_k
    @field_validator("top_k")
    @classmethod
    def validate_top_k(cls, value: int) -> int:
        if not (1 <= value <= 100):
            raise ValueError("top_k must be between 1 and 100")
        return value


class SourceReference(BaseModel):
    source_name: str | None = None
    source_type: str | None = None
    url: str | None = None
    score: float | None = None
    title: str | None = None
    text: str | None = None
    extras: dict | None = None


class QueryResponse(BaseModel):
    references: list[SourceReference]
    raw: list[str] | None = None
