from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.http import RetrySession
from utils.parse import parse_list, parse_timestamp
from utils.text import slugify

logger = logging.getLogger(__name__)

GITBOOK_API_BASE = "https://api.gitbook.com/v1"


class GitBookClient:
    """REST client for the GitBook API v1."""

    def __init__(self, api_token: str) -> None:
        self._headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        }
        self._session = RetrySession()

    def list_orgs(self) -> list[dict]:
        """Return all organizations accessible to the token."""
        resp = self._session.get(f"{GITBOOK_API_BASE}/orgs", headers=self._headers)
        resp.raise_for_status()
        return resp.json().get("items", [])

    def list_spaces(self) -> list[dict]:
        """Return all spaces accessible to the token across all organizations."""
        spaces: list[dict] = []
        for org in self.list_orgs():
            org_id = org.get("id")
            if not org_id:
                continue
            url: str | None = f"{GITBOOK_API_BASE}/orgs/{org_id}/spaces"
            while url:
                resp = self._session.get(url, headers=self._headers)
                resp.raise_for_status()
                data = resp.json()
                spaces.extend(data.get("items", []))
                next_cursor = data.get("next", {}).get("page") if isinstance(data.get("next"), dict) else None
                url = f"{GITBOOK_API_BASE}/orgs/{org_id}/spaces?page={next_cursor}" if next_cursor else None
        return spaces

    def list_pages(self, space_id: str) -> list[dict]:
        """Return the flat page list for a space."""
        resp = self._session.get(
            f"{GITBOOK_API_BASE}/spaces/{space_id}/content/pages",
            headers=self._headers,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("pages", [])

    def get_page(self, space_id: str, page_id: str) -> dict:
        """Return the page with its content rendered as Markdown in the `markdown` field."""
        resp = self._session.get(
            f"{GITBOOK_API_BASE}/spaces/{space_id}/content/page/{page_id}",
            params={"format": "markdown"},
            headers=self._headers,
        )
        resp.raise_for_status()
        return resp.json()

    def close(self) -> None:
        self._session.close()


def _flatten_pages(pages: list[dict]) -> Iterator[dict]:
    """Yield all pages recursively, including nested sub-pages."""
    for page in pages:
        yield page
        children = page.get("pages", [])
        if children:
            yield from _flatten_pages(children)


class GitBookIngestionJob(IngestionJob):
    """Ingestion connector for GitBook spaces.

    Fetches pages via the GitBook REST API and stores them in the vector store.

    Configuration (config.yaml):
        - config.api_token: GitBook API token (required)
        - config.space_ids: list of space IDs to ingest (optional, comma-separated
          string or list); if omitted, all spaces accessible to the token are ingested
    """

    @property
    def source_type(self) -> str:
        return "gitbook"

    def __init__(self, config: dict) -> None:
        super().__init__(config)

        cfg = config.get("config", {})

        api_token = cfg.get("api_token", "").strip()
        if not api_token:
            raise ValueError("api_token is required in GitBook connector config")

        self.space_ids: list[str] = parse_list(cfg.get("space_ids", []))
        self._client = GitBookClient(api_token)

    def list_items(self) -> Iterator[IngestionItem]:
        try:
            try:
                space_ids = self._resolve_space_ids()
            except Exception as exc:
                logger.error(f"[{self.source_name}] Failed to resolve space IDs: {exc}")
                return

            total = 0
            for space_id in space_ids:
                try:
                    pages = self._client.list_pages(space_id)
                except Exception as exc:
                    logger.error(f"[{self.source_name}] Failed to list pages for space {space_id!r}: {exc}")
                    continue

                for page in _flatten_pages(pages):
                    page_id = page.get("id")
                    if not page_id:
                        logger.warning(
                            f"[{self.source_name}] Skipping page with missing id in space {space_id!r}: "
                            f"title={page.get('title')!r} slug={page.get('slug')!r}"
                        )
                        continue

                    page["_space_id"] = space_id
                    updated_at = parse_timestamp(page.get("updatedAt"))
                    yield IngestionItem(
                        id=f"gitbook:{space_id}:{page_id}",
                        source_ref=page,
                        last_modified=updated_at,
                    )
                    total += 1

            logger.info(f"[{self.source_name}] Found {total} page(s)")
        finally:
            self._client.close()

    def get_raw_content(self, item: IngestionItem) -> str:
        page = item.source_ref
        space_id = page.get("_space_id", "")
        page_id = page.get("id", "")
        title = page.get("title") or ""

        try:
            detail = self._client.get_page(space_id, page_id)
        except Exception as exc:
            logger.error(f"[{self.source_name}] Failed to fetch page {page_id!r} in space {space_id!r}: {exc}")
            return ""

        markdown = detail.get("markdown")
        content = markdown.strip() if isinstance(markdown, str) else ""

        if not content:
            logger.warning(f"[{self.source_name}] Empty content for page {page_id!r} in space {space_id!r}")
            return ""

        first_line = content.split("\n", 1)[0].lstrip("# ").strip()
        if title and first_line.lower() != title.lower():
            return f"# {title}\n\n{content}"
        return content

    def get_item_name(self, item: IngestionItem) -> str:
        page = item.source_ref
        space_id = page.get("_space_id", "")
        page_id = page.get("id", "")
        return slugify(f"gitbook-{space_id}-{page_id}", max_len=255)

    def get_item_checksum(self, item: IngestionItem) -> str | None:
        updated_at = item.source_ref.get("updatedAt")
        if not updated_at:
            return None
        page_id = item.source_ref.get("id", "")
        space_id = item.source_ref.get("_space_id", "")
        return f"{space_id}:{page_id}:{updated_at}"

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        page = item.source_ref
        parent_id = page.get("parent") or page.get("parentId") or ""
        return {
            "space_id": page.get("_space_id", ""),
            "page_id": str(page.get("id", "")),
            "page_title": page.get("linkTitle") or page.get("title") or "",
            "page_url": (page.get("urls") or {}).get("app") or "",
            "parent_page_id": str(parent_id) if parent_id else "",
            "description": page.get("description") or "",
            "tags": self._tag_slugs(page.get("tags")),
            "created_at": page.get("createdAt") or "",
            "updated_at": page.get("updatedAt") or "",
        }

    @staticmethod
    def _tag_slugs(tags: Any) -> list[str]:
        """Return the slugs of the page tags, skipping malformed entries."""
        slugs: list[str] = []
        for entry in tags or []:
            tag = entry.get("tag") if isinstance(entry, dict) else None
            slug = tag.get("tag") if isinstance(tag, dict) else None
            if slug:
                slugs.append(slug)
        return slugs

    def _resolve_space_ids(self) -> list[str]:
        """Return configured space IDs, or fetch all accessible ones if none configured."""
        if self.space_ids:
            return self.space_ids
        spaces = self._client.list_spaces()
        ids = [s["id"] for s in spaces if s.get("id")]
        logger.info(f"[{self.source_name}] Discovered {len(ids)} accessible space(s)")
        return ids
