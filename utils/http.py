from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable
from typing import Any

import requests

logger = logging.getLogger(__name__)

# Full URLs, and the scheme-less ``url: /path?query`` that ``requests`` puts in connection errors.
# ``requests`` percent-encodes whitespace, so a URL or path ends at the next whitespace. Characters such as
# quotes, parentheses and apostrophes can be part of a file name, so they do not end the match.
_URL_PATTERN = re.compile(r"https?://\S+|(?<=url: )\S+")


class UrlRedactingFilter(logging.Filter):
    """Logging filter that replaces URLs in a record, so a third-party logger cannot write signed URLs."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = RetrySession.redact_urls(record.getMessage())
        record.args = None
        return True


class RetrySession:
    """Thin HTTP client with exponential backoff and 429 / 5xx retry logic.

    Handles:
    - Network errors: exponential backoff (2**attempt seconds)
    - HTTP 429: honours Retry-After header, falls back to exponential backoff
    - HTTP 5xx: retries up to max_retries times

    ``before_request`` is called before every HTTP attempt, including retries, so callers can
    apply a rate limiter to each request that is sent.
    """

    def __init__(
        self, max_retries: int = 3, timeout: int = 30, before_request: Callable[[], None] | None = None
    ) -> None:
        self.max_retries = max_retries
        self.timeout = timeout
        self._before_request = before_request
        self._session = requests.Session()

    def get(
        self, url: str, *, params: Any = None, headers: dict | None = None, stream: bool = False
    ) -> requests.Response:
        return self._request("GET", url, params=params, headers=headers, stream=stream)

    def post(
        self,
        url: str,
        *,
        json: Any = None,
        data: Any = None,
        headers: dict | None = None,
        retry: bool = False,
    ) -> requests.Response:
        if retry:
            return self._request("POST", url, json=json, data=data, headers=headers)
        return self._session.request("POST", url, json=json, data=data, headers=headers or {}, timeout=self.timeout)

    @staticmethod
    def redact_urls(message: str) -> str:
        """Replace URLs in an exception message, which can carry signed download tokens."""
        return _URL_PATTERN.sub("<url>", message)

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        headers = kwargs.pop("headers", None) or {}
        last_exc: Exception | None = None

        for attempt in range(self.max_retries + 1):
            if self._before_request:
                self._before_request()
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
                        self.redact_urls(str(exc)),
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
