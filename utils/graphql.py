from __future__ import annotations

from typing import Any

import requests


class GraphQLError(Exception):
    """Raised when a GraphQL response contains errors."""


def graphql_request(
    url: str,
    query: str,
    variables: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 30,
) -> dict[str, Any]:
    """Execute a GraphQL query and return the response data dict.

    Raises:
        requests.HTTPError: on non-2xx HTTP responses.
        GraphQLError: when the response contains a GraphQL-level errors field.
    """
    payload = {"query": query, "variables": variables or {}}
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
