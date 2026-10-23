from typing import Any

from pydantic import BaseModel, field_validator

VALID_OPERATORS = {
    "==",
    "!=",
    ">",
    ">=",
    "<",
    "<=",
    "in",
    "nin",
    "text_match",
    "text_match_insensitive",
    "contains",
}


class MetadataFilterInput(BaseModel):
    name: str
    operator: str
    value: Any = None

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v: str) -> str:
        if v not in VALID_OPERATORS:
            raise ValueError(f"Unsupported operator '{v}'. Valid operators: {sorted(VALID_OPERATORS)}")
        return v


class DocumentRequest(BaseModel):
    document_id: str
    max_chunks: int = 10
    max_chunk_length: int = 2000

    @field_validator("max_chunks")
    @classmethod
    def validate_max_chunks(cls, v: int) -> int:
        if v < 1:
            raise ValueError("max_chunks must be at least 1")
        return v

    @field_validator("max_chunk_length")
    @classmethod
    def validate_max_chunk_length(cls, v: int) -> int:
        if v < 1:
            raise ValueError("max_chunk_length must be at least 1")
        return v


class DocumentsRequest(BaseModel):
    metadata_filters: list[MetadataFilterInput] = []
    max_chunks: int = 10
    max_chunk_length: int = 2000
    max_documents: int = 10

    @field_validator("max_chunks")
    @classmethod
    def validate_max_chunks(cls, v: int) -> int:
        if v < 1:
            raise ValueError("max_chunks must be at least 1")
        return v

    @field_validator("max_chunk_length")
    @classmethod
    def validate_max_chunk_length(cls, v: int) -> int:
        if v < 1:
            raise ValueError("max_chunk_length must be at least 1")
        return v

    @field_validator("max_documents")
    @classmethod
    def validate_max_documents(cls, v: int) -> int:
        if v < 1:
            raise ValueError("max_documents must be at least 1")
        return v


class ChunkOut(BaseModel):
    text: str
    metadata: dict


class DocumentOut(BaseModel):
    document_id: str
    metadata: dict
    chunks: list[ChunkOut]
