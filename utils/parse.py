from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


def parse_timestamp(value: Any) -> datetime | None:
    """Parse an ISO-8601 string into a datetime, or pass through an existing datetime.

    Handles the Jira/GitLab convention of ending with 'Z' and '+0000' offsets.
    Returns None on any parse failure instead of raising.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        normalized = (value[:-1] + "+00:00") if value.endswith("Z") else value
        return datetime.fromisoformat(normalized)
    except (ValueError, TypeError):
        return None


def parse_list(value: Any, *, lower: bool = False) -> list[str]:
    """Parse a comma-separated string or an existing list into a list of strings.

    Strips whitespace from each item and filters empty strings.
    With lower=True each item is lowercased (useful for extension sets).
    Handles native Python lists from DB JSON and strings from YAML transparently.
    """
    if value is None:
        return []
    if isinstance(value, list | tuple | set):
        items = [str(v).strip() for v in value if v is not None]
    else:
        items = [v.strip() for v in str(value).split(",")]
    items = [v for v in items if v]
    if lower:
        items = [v.lower() for v in items]
    return items


def parse_bool(value: Any, default: bool = False) -> bool:
    """Parse a boolean from None, a native bool, or a truthy string.

    Truthy strings: 'true', 'yes', '1', 'on' (case-insensitive).
    Handles native Python bools from DB JSON and strings from YAML transparently.
    Returns default when value is None.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value

    normalized = str(value).strip().lower()
    if normalized in {"true", "yes", "1", "on"}:
        return True
    if normalized in {"false", "no", "0", "off"}:
        return False
    return default
