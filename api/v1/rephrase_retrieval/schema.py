from pydantic import BaseModel, field_validator

from api.v1.shared_schema import SourceReference, TopK


class QueryRequest(BaseModel):
    query: str
    top_k: TopK = 20

    @field_validator("query")
    @classmethod
    def validate_query(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("Query cannot be empty")
        return value


class QueryResponse(BaseModel):
    answer: str
    references: list[SourceReference]
