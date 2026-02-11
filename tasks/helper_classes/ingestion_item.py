from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class IngestionItem:
    id: str
    source_ref: Any
    last_modified: datetime | None = None
    url: str | None = None
    last_modified: datetime | None = None
    # Mutable field for caching additional metadata during processing
    # Excluded from equality and hashing to keep the dataclass hashable
    _metadata_cache: dict[str, Any] = field(default_factory=dict, init=False, compare=False, hash=False)
