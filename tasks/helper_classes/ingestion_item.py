from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Any, Dict

@dataclass(frozen=True)
class IngestionItem:
    id: str
    source_ref: Any
    last_modified: Optional[datetime] = None
    url: Optional[str] = None
