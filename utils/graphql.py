from __future__ import annotations

from typing import Any

import requests

from utils.http import RetrySession


class GraphQLError(Exception):
    """Raised when a GraphQL response contains errors."""


def graphql_request(
    url: str,
    query: str,
    variables: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 30,
    session: RetrySession | None = None,
) -> dict[str, Any]:
    """Execute a GraphQL query and return the response data dict.

    If `session` is provided, the request is retried on network errors, 429s, and
    5xx responses using the session's retry/backoff policy; `timeout` is ignored
    in favor of the session's own configured timeout. Only pass a session for
    operations safe to retry (idempotent queries, or mutations designed to
    tolerate retries) — retries are unconditional and can duplicate side effects
    for non-idempotent mutations.

    Raises:
        requests.HTTPError: on non-2xx HTTP responses.
        GraphQLError: when the response contains a GraphQL-level errors field.
    """
    payload = {"query": query, "variables": variables or {}}
    if session is not None:
        resp = session.post(url, json=payload, headers=headers or {}, retry=True)
    else:
        resp = requests.post(url, json=payload, headers=headers or {}, timeout=timeout)
    resp.raise_for_status()

    try:
        body: dict[str, Any] = resp.json()
    except ValueError as exc:
        raise GraphQLError(f"Non-JSON response: {resp.text[:200]}") from exc

    if errors := body.get("errors"):
        messages = "; ".join(e.get("message", str(e)) if isinstance(e, dict) else str(e) for e in errors)
        raise GraphQLError(f"GraphQL errors: {messages}")

    return body.get("data", {})
