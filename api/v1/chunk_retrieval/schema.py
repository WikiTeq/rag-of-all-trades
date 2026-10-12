import re
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

from api.v1.shared_schema import SourceReference

_SAFE_NAME_PATTERN = re.compile(r"^[0-9a-zA-Z.\-_ ]+$")
_SAFE_VALUE_PATTERN = re.compile(r"^[0-9a-zA-Z.\-_;,:?!\[\]=@() ]+$")


def _validate_name(v: str) -> str:
    if not _SAFE_NAME_PATTERN.match(v):
        raise ValueError("name contains invalid characters; allowed: 0-9 a-z A-Z . - _ space")
    return v


def _validate_value(v: str) -> str:
    if not _SAFE_VALUE_PATTERN.match(v):
        raise ValueError("value contains invalid characters")
    return v


class ScalarMetadataFilter(BaseModel):
    name: str
    operator: Literal["EQ", "NE", "GT", "GTE", "LT", "LTE", "TEXT_MATCH", "CONTAINS"]
    value: str | int | float

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        return _validate_name(v)

    @field_validator("value", mode="before")
    @classmethod
    def validate_value(cls, v: object) -> object:
        if isinstance(v, str):
            _validate_value(v)
        return v


class ListMetadataFilter(BaseModel):
    name: str
    operator: Literal["IN", "NIN", "ANY", "ALL"]
    value: list[str | int | float]

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        return _validate_name(v)

    @field_validator("value", mode="before")
    @classmethod
    def validate_value(cls, v: object) -> object:
        if isinstance(v, list):
            for item in v:
                if isinstance(item, str):
                    _validate_value(item)
        return v


MetadataFilterItem = Annotated[
    ScalarMetadataFilter | ListMetadataFilter,
    Field(discriminator="operator"),
]


class QueryRequest(BaseModel):
    query: str = Field(pattern=r"\S")
    top_k: int = Field(default=20, ge=1, le=100)
    metadata_filters: list[MetadataFilterItem] | None = None


class QueryResponse(BaseModel):
    references: list[SourceReference]
    raw: list[str] | None = None
