from pydantic import BaseModel
from typing import List, Optional, Dict

#Request Model
class QueryRequest(BaseModel):
    query: str

#Source Reference Model
class SourceReference(BaseModel):
    source_name: Optional[str] = None
    source_type: Optional[str] = None
    url: Optional[str] = None
    score: Optional[float] = None
    title: Optional[str] = None
    text: Optional[str] = None
    extras: Optional[Dict] = None

class QueryResponse(BaseModel):
    answer: str
    references: List[SourceReference]
