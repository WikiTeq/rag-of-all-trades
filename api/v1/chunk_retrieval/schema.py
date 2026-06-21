import re
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

_SAFE_PATTERN = re.compile(r"^[0-9a-zA-Z.\-_]+$")


def _validate_safe_string(v: str, field: str) -> str:
    if not _SAFE_PATTERN.match(v):
        raise ValueError(f"{field} contains invalid characters; allowed: 0-9 a-z A-Z . - _")
    return v


class ScalarMetadataFilter(BaseModel):
    name: str
    operator: Literal["EQ", "NE", "GT", "GTE", "LT", "LTE", "TEXT_MATCH"]
    value: str | int | float

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        return _validate_safe_string(v, "name")

    @field_validator("value", mode="before")
    @classmethod
    def validate_value(cls, v: object) -> object:
        if isinstance(v, str):
            _validate_safe_string(v, "value")
        return v


class ListMetadataFilter(BaseModel):
    name: str
    operator: Literal["IN", "NIN"]
    value: list[str | int | float]

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        return _validate_safe_string(v, "name")

    @field_validator("value", mode="before")
    @classmethod
    def validate_value(cls, v: object) -> object:
        if isinstance(v, list):
            for item in v:
                if isinstance(item, str):
                    _validate_safe_string(item, "value item")
        return v


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
