from pydantic import BaseModel, Field

from api.v1.shared_schema import SourceReference


class QueryRequest(BaseModel):
    query: str = Field(pattern=r"\S")
    top_k: int = Field(default=20, ge=1, le=100)


class QueryResponse(BaseModel):
    answer: str
    references: list[SourceReference]
