from __future__ import annotations

import logging
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)


class RetrySession:
    """Thin HTTP client with exponential backoff and 429 / 5xx retry logic.

    Handles:
    - Network errors: exponential backoff (2**attempt seconds)
    - HTTP 429: honours Retry-After header, falls back to exponential backoff
    - HTTP 5xx: retries up to max_retries times
    """

    def __init__(self, max_retries: int = 3, timeout: int = 30) -> None:
        self.max_retries = max_retries
        self.timeout = timeout
        self._session = requests.Session()

    def get(self, url: str, *, params: Any = None, headers: dict | None = None) -> requests.Response:
        return self._request("GET", url, params=params, headers=headers)

    def post(
        self, url: str, *, json: Any = None, headers: dict | None = None, retry: bool = False
    ) -> requests.Response:
        if retry:
            return self._request("POST", url, json=json, headers=headers)
        return self._session.request("POST", url, json=json, headers=headers or {}, timeout=self.timeout)

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        headers = kwargs.pop("headers", None) or {}
        last_exc: Exception | None = None

        for attempt in range(self.max_retries + 1):
            try:
                resp = self._session.request(method, url, headers=headers, timeout=self.timeout, **kwargs)
            except requests.RequestException as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    wait = 2**attempt
                    logger.warning(
                        "Request error (attempt %d/%d): %s — retrying in %ds",
                        attempt + 1,
                        self.max_retries + 1,
                        exc,
                        wait,
                    )
                    time.sleep(wait)
                continue

            if resp.status_code == 429:
                if attempt < self.max_retries:
                    try:
                        wait = int(resp.headers.get("Retry-After", 2**attempt))
                    except (ValueError, TypeError):
                        wait = 2**attempt
                    logger.warning("Rate-limited (429) — retrying in %ds", wait)
                    resp.close()
                    time.sleep(wait)
                continue

            if resp.status_code >= 500 and attempt < self.max_retries:
                wait = 2**attempt
                logger.warning(
                    "Server error %d (attempt %d/%d) — retrying in %ds",
                    resp.status_code,
                    attempt + 1,
                    self.max_retries + 1,
                    wait,
                )
                resp.close()
                time.sleep(wait)
                continue

            return resp

        if last_exc:
            raise last_exc
        assert resp is not None, "unreachable: no exception but also no response"
        return resp

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> RetrySession:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
