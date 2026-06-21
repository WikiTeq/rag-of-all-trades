from typing import Any

from pydantic import BaseModel, field_validator

from api.v1.shared_schema import SourceReference


class QueryRequest(BaseModel):
    query: str
    top_k: int = 20
    metadata_filters: dict[str, Any] | None = None

    @field_validator("query")
    @classmethod
    def validate_query(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("Query cannot be empty")
        return value

    @field_validator("top_k")
    @classmethod
    def validate_top_k(cls, value: int) -> int:
        if not (1 <= value <= 100):
            raise ValueError("top_k must be between 1 and 100")
        return value


class QueryResponse(BaseModel):
    references: list[SourceReference]
    raw: list[str] | None = None
