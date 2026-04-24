from __future__ import annotations

import logging
from collections.abc import Callable, Hashable
from typing import Generic, TypeVar

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


class CachedResolver(Generic[K, V]):
    """Cache-aside resolver for API lookups (user names, pipeline stages, etc.).

    On the first access for a key, fetch_fn is called.  The result (including
    None) is cached so subsequent lookups are O(1).  Failures in fetch_fn are
    logged as warnings and None is returned; the failed key is NOT cached so
    the next caller gets a fresh attempt.
    """

    def __init__(
        self,
        fetch_fn: Callable[[K], V | None],
        logger: logging.Logger | None = None,
        source_name: str = "",
    ) -> None:
        self._fetch = fetch_fn
        self._cache: dict[K, V | None] = {}
        self._log = logger or logging.getLogger(__name__)
        self._source = source_name

    def resolve(self, key: K) -> V | None:
        if key in self._cache:
            return self._cache[key]
        try:
            value = self._fetch(key)
        except Exception as exc:
            self._log.warning("[%s] Failed to resolve key %r: %s", self._source, key, exc)
            return None
        self._cache[key] = value
        return value

    def clear(self) -> None:
        self._cache.clear()
