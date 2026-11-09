from pydantic import BaseModel, field_validator

from utils.api import validate_similarity_cutoff


# Request Model
class QueryRequest(BaseModel):
    query: str
    top_k: int = 20
    similarity_cutoff: float | None = None

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

    _validate_similarity_cutoff = field_validator("similarity_cutoff")(validate_similarity_cutoff)


# Source Reference Model
class SourceReference(BaseModel):
    source_name: str | None = None
    source_type: str | None = None
    url: str | None = None
    score: float | None = None
    title: str | None = None
    text: str | None = None
    extras: dict | None = None


class QueryResponse(BaseModel):
    answer: str
    references: list[SourceReference]
