from pydantic import BaseModel, field_validator
from typing import List, Optional, Dict, Any

class QueryRequest(BaseModel):
    query: str
    top_k: int = 5
    metadata_filters: Optional[Dict[str, Any]] = None
    
    # Validator for top_k
    @field_validator("top_k")
    @classmethod
    def validate_top_k(cls, value: int) -> int:
        if not (1 <= value <= 20):
            raise ValueError("top_k must be between 1 and 20")
        return value

class SourceReference(BaseModel):
    source_name: Optional[str] = None
    source_type: Optional[str] = None
    url: Optional[str] = None
    score: Optional[float] = None
    title: Optional[str] = None
    text: Optional[str] = None
    extras: Optional[Dict] = None

class QueryResponse(BaseModel):
    references: List[SourceReference]
    raw: Optional[List[str]] = None
