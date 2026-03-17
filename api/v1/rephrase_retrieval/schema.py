from pydantic import BaseModel


# Request Model
class QueryRequest(BaseModel):
    query: str


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
