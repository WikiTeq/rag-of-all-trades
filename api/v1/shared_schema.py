from typing import Annotated

from pydantic import BaseModel, Field

TopK = Annotated[int, Field(ge=1, le=100)]


class SourceReference(BaseModel):
    source_name: str | None = None
    source_type: str | None = None
    url: str | None = None
    score: float | None = None
    title: str | None = None
    text: str | None = None
    extras: dict | None = None
