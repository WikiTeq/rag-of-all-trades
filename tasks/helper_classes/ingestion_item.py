from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Any, Dict

@dataclass(frozen=True)
class IngestionItem:
    id: str
    source_ref: Any
    last_modified: Optional[datetime] = None
    # Mutable field for caching additional metadata during processing
    # Excluded from equality and hashing to keep the dataclass hashable
    _metadata_cache: Dict[str, Any] = field(default_factory=dict, init=False, compare=False, hash=False)
