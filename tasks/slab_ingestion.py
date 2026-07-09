import json
import logging
import time
from collections.abc import Iterator
from typing import Any

import requests.exceptions

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.graphql import graphql_request
from utils.parse import parse_list, parse_timestamp
from utils.text import slugify

logger = logging.getLogger(__name__)

SLAB_GRAPHQL_URL = "https://api.slab.com/v1/graphql"

_QUERY_SEARCH_POSTS = """
    query SearchPosts($first: Int!, $after: String) {
        search(query: "", first: $first, after: $after, types: [POST]) {
            edges {
                node {
                    ... on PostSearchResult {
                        post {
                            id
                            title
                            content
                            updatedAt
                        }
                    }
                }
            }
            pageInfo {
                endCursor
                hasNextPage
            }
        }
    }
"""

_QUERY_GET_TOPIC = """
    query GetTopicPosts($topicId: ID!, $first: Int!, $after: String) {
        topic(id: $topicId) {
            id
            name
            parent { id name }
            ancestors { id name }
            posts(first: $first, after: $after) {
                edges {
                    node {
                        id
                        title
                        updatedAt
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
    }
"""

_QUERY_GET_POST = """
    query GetPost($postId: ID!) {
        post(id: $postId) {
            id
            title
            content
            updatedAt
        }
    }
"""

_QUERY_ORG_HOST = """
    query GetOrgHost {
        organization {
            host
        }
    }
"""


class SlabGraphQLClient:
    """Thin GraphQL client for the Slab API with retry logic."""

    def __init__(self, api_token: str, max_retries: int, retry_delay: float, source_name: str):
        self._headers = {
            "Authorization": api_token,
            "Content-Type": "application/json",
        }
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._source_name = source_name

    def execute(self, query: str, variables: dict[str, Any] | None = None) -> dict:
        last_exc: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                return graphql_request(
                    SLAB_GRAPHQL_URL,
                    query,
                    variables=variables,
                    headers=self._headers,
                    timeout=60,
                )
            except (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError,
            ) as e:
                last_exc = e
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"[{self._source_name}] Slab GraphQL error (attempt {attempt + 1}): {e}. Retrying..."
                    )
                    time.sleep(self.retry_delay)
        raise RuntimeError(
            f"[{self._source_name}] Slab GraphQL request failed after {self.max_retries} attempts: {last_exc}"
        )


class SlabIngestionJob(IngestionJob):
    """Ingestion connector for Slab knowledge-base posts.

    Fetches posts via the Slab GraphQL API and stores them in the vector
    store. When ``topic_ids`` is configured, only posts belonging to those
    topics are ingested; otherwise all organisation posts are fetched using
    cursor-based pagination.

    Configuration (config.yaml):
        - config.api_token: Slab bot/API token (required)
        - config.topic_ids: comma-separated topic IDs to filter (optional)
        - config.max_retries: max GraphQL retry attempts on failure (optional, default 3)
        - config.retry_delay: seconds between retries (optional, default 2)
        - config.search_batch_size: posts per search/topic page (optional, default 100)
    """

    @property
    def source_type(self) -> str:
        return "slab"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        api_token = cfg.get("api_token", "").strip()
        if not api_token:
            raise ValueError("api_token is required in Slab connector config")

        self.topic_ids: list[str] = parse_list(cfg.get("topic_ids"))

        self.search_batch_size = int(cfg.get("search_batch_size", 100))
        if self.search_batch_size <= 0:
            raise ValueError("search_batch_size must be > 0")

        max_retries = int(cfg.get("max_retries", 3))
        if max_retries < 1:
            raise ValueError("max_retries must be >= 1")

        retry_delay = float(cfg.get("retry_delay", 2))
        if retry_delay < 0:
            raise ValueError("retry_delay must be >= 0")

        self._client = SlabGraphQLClient(
            api_token=api_token,
            max_retries=max_retries,
            retry_delay=retry_delay,
            source_name=self.source_name,
        )
        self._org_host: str | None = None

        logger.info(f"[{self.source_name}] Initialized Slab connector (topic_ids={self.topic_ids or 'all'})")

    # ------------------------------------------------------------------
    # IngestionJob abstract method implementations
    # ------------------------------------------------------------------

    def list_items(self) -> Iterator[IngestionItem]:
        if self.topic_ids:
            yield from self._list_by_topics()
        else:
            yield from self._list_all_via_search()

    def get_raw_content(self, item: IngestionItem) -> str:
        post = item.source_ref
        title = post.get("title") or ""
        content = self._extract_content(post.get("content") or "")
        parts = []
        if title:
            parts.append(f"# {title}")
        if content.strip():
            parts.append(content)
        return "\n\n".join(parts)

    def get_item_name(self, item: IngestionItem) -> str:
        return slugify(item.id, max_len=255)

    def get_extra_metadata(self, item: IngestionItem, _content: str, _metadata: dict[str, Any]) -> dict[str, Any]:
        post = item.source_ref
        topic = post.get("_topic_meta") or {}
        return {
            "url": f"https://{self._get_org_host()}/posts/{item.id}",
            "title": post.get("title") or "",
            "topic_id": topic.get("id", ""),
            "topic_name": topic.get("name", ""),
            "topic_parent_id": topic.get("parent_id", ""),
            "topic_parent_name": topic.get("parent_name", ""),
            "topic_ancestors": topic.get("ancestors", []),
        }

    def _get_org_host(self) -> str:
        """Return the organization's Slab host (e.g. my-team.slab.com), fetched once and cached.

        Post URLs are only reachable under the organization's own subdomain, not
        the bare slab.com domain, so this must be resolved via the API rather
        than hardcoded.
        """
        if self._org_host is None:
            data = self._client.execute(_QUERY_ORG_HOST)
            host = ((data or {}).get("organization") or {}).get("host")
            if not host:
                raise RuntimeError(f"[{self.source_name}] Slab API did not return an organization host")
            self._org_host = host
        return self._org_host

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _list_all_via_search(self) -> Iterator[IngestionItem]:
        cursor: str | None = None
        total = 0
        while True:
            data = self._client.execute(
                _QUERY_SEARCH_POSTS,
                {"first": self.search_batch_size, "after": cursor},
            )
            search = (data or {}).get("search") or {}
            page_info = search.get("pageInfo", {})

            for edge in search.get("edges", []):
                post = (edge.get("node") or {}).get("post") or {}
                if not post.get("id"):
                    continue
                yield self._make_item(post)
                total += 1

            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

        logger.info(f"[{self.source_name}] Found {total} post(s) via search")

    def _list_by_topics(self) -> Iterator[IngestionItem]:
        total = 0
        for topic_id in self.topic_ids:
            cursor: str | None = None
            topic_meta: dict | None = None

            while True:
                data = self._client.execute(
                    _QUERY_GET_TOPIC,
                    {"topicId": topic_id, "first": self.search_batch_size, "after": cursor},
                )
                topic = (data or {}).get("topic") or {}

                if topic_meta is None:
                    topic_meta = {
                        "id": topic.get("id", ""),
                        "name": topic.get("name", ""),
                        "parent_id": (topic.get("parent") or {}).get("id", ""),
                        "parent_name": (topic.get("parent") or {}).get("name", ""),
                        "ancestors": [
                            {"id": a.get("id", ""), "name": a.get("name", "")} for a in (topic.get("ancestors") or [])
                        ],
                    }

                posts_conn = topic.get("posts") or {}
                page_info = posts_conn.get("pageInfo", {})

                for edge in posts_conn.get("edges", []):
                    stub = edge.get("node") or {}
                    post_id = stub.get("id")
                    if not post_id:
                        continue
                    post_data = self._client.execute(_QUERY_GET_POST, {"postId": post_id})
                    post = (post_data or {}).get("post") or {}
                    if not post.get("id"):
                        continue
                    post["_topic_meta"] = topic_meta
                    yield self._make_item(post)
                    total += 1

                if not page_info.get("hasNextPage"):
                    break
                cursor = page_info.get("endCursor")

        logger.info(f"[{self.source_name}] Found {total} post(s) from {len(self.topic_ids)} topic(s)")

    @staticmethod
    def _extract_content(raw: str) -> str:
        """Extract plain text from Slab content.

        Slab stores content as a Quill delta JSON array of insert ops.
        Each op has an ``insert`` key that is either a plain string or an
        embedded object (image, hr, etc.) which we skip.
        Falls back to returning the raw value as a string if it is not valid JSON.
        """
        if not raw:
            return ""
        try:
            ops = json.loads(raw)
        except (ValueError, TypeError):
            return str(raw)
        if isinstance(ops, dict) and isinstance(ops.get("ops"), list):
            ops = ops["ops"]
        if not isinstance(ops, list):
            return str(ops)
        parts = []
        for op in ops:
            insert = op.get("insert") if isinstance(op, dict) else None
            if isinstance(insert, str):
                parts.append(insert)
        return "".join(parts).strip()

    @staticmethod
    def _make_item(post: dict) -> IngestionItem:
        post_id = post["id"]
        updated_at = parse_timestamp(post.get("updatedAt"))
        return IngestionItem(
            id=post_id,
            source_ref=post,
            last_modified=updated_at,
        )
