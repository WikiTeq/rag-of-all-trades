from dataclasses import dataclass, field
from enum import StrEnum


class IngestionItemStatus(StrEnum):
    """Outcome of fetching the content of one ingestion item."""

    SUCCESS = "success"
    FAIL = "fail"


@dataclass(frozen=True)
class IngestionItemResult:
    """Explicit outcome of ``get_raw_content``.

    A connector returns it instead of a plain string to tell a successful fetch with
    empty content (``SUCCESS`` and an empty ``result``) from a failed fetch (``FAIL``
    and ``errors``). A plain string keeps its old meaning: an empty string is skipped
    and tried again on the next run.
    """

    status: IngestionItemStatus
    result: str = ""
    errors: tuple[str, ...] = field(default_factory=tuple)
