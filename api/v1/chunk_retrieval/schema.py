from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator


class ScalarMetadataFilter(BaseModel):
    name: str
    operator: Literal["EQ", "NE", "GT", "GTE", "LT", "LTE", "TEXT_MATCH"]
    value: str | int | float


class ListMetadataFilter(BaseModel):
    name: str
    operator: Literal["IN", "NIN"]
    value: list[str | int | float]


MetadataFilterItem = Annotated[
    ScalarMetadataFilter | ListMetadataFilter,
    Field(discriminator="operator"),
]


class QueryRequest(BaseModel):
    query: str
    top_k: int = 20
    metadata_filters: list[MetadataFilterItem] | None = None

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
