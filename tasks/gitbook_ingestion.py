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
        """Return the full page document including content node tree."""
        resp = self._session.get(
            f"{GITBOOK_API_BASE}/spaces/{space_id}/content/page/{page_id}",
            headers=self._headers,
        )
        resp.raise_for_status()
        return resp.json()

    def close(self) -> None:
        self._session.close()


class GitBookMarkdownConverter:
    """Converts a GitBook document node tree to Markdown."""

    @staticmethod
    def extract(document: dict) -> str:
        nodes = document.get("nodes", [])
        if not nodes:
            return ""
        return GitBookMarkdownConverter._nodes(nodes)

    @staticmethod
    def _nodes(nodes: list[dict]) -> str:
        parts: list[str] = []
        for node in nodes:
            parts.append(GitBookMarkdownConverter._node(node))
        return "\n".join(p for p in parts if p)

    @staticmethod
    def _leaf(leaf: dict) -> str:
        text = leaf.get("text", "") or ""
        marks = {m.get("type") for m in leaf.get("marks", [])}
        if "bold" in marks:
            text = f"**{text}**"
        if "italic" in marks:
            text = f"*{text}*"
        if "code" in marks:
            text = f"`{text}`"
        return text

    @staticmethod
    def _node(node: dict) -> str:
        node_type = node.get("type", "")
        children = node.get("nodes", [])

        # Text node: may use legacy `text`+`marks` or newer `leaves` array
        if node.get("object") == "text":
            leaves = node.get("leaves")
            if leaves is not None:
                return "".join(GitBookMarkdownConverter._leaf(lf) for lf in leaves)
            text = node.get("text", "") or ""
            marks = {m.get("type") for m in node.get("marks", [])}
            if "bold" in marks:
                text = f"**{text}**"
            if "italic" in marks:
                text = f"*{text}*"
            if "code" in marks:
                text = f"`{text}`"
            return text

        if node_type == "text":
            text = node.get("text", "") or ""
            marks = {m.get("type") for m in node.get("marks", [])}
            if "bold" in marks:
                text = f"**{text}**"
            if "italic" in marks:
                text = f"*{text}*"
            if "code" in marks:
                text = f"`{text}`"
            return text

        if node_type == "paragraph":
            inner = GitBookMarkdownConverter._inline(children)
            return f"{inner}\n" if inner.strip() else ""

        if node_type == "blockquote":
            inner = GitBookMarkdownConverter._nodes(children)
            return "\n".join(f"> {line}" for line in inner.splitlines()) + "\n" if inner.strip() else ""

        if node_type.startswith("heading-"):
            try:
                level = int(node_type.split("-")[1])
            except (IndexError, ValueError):
                level = 1
            inner = GitBookMarkdownConverter._inline(children)
            return f"{'#' * level} {inner}\n"

        if node_type in ("list-unordered", "list-ordered"):
            ordered = node_type == "list-ordered"
            items: list[str] = []
            for i, item in enumerate(children):
                bullet = f"{i + 1}." if ordered else "-"
                item_nodes = item.get("nodes", [])
                item_text = GitBookMarkdownConverter._nodes(item_nodes).strip()
                # Indent continuation lines so they stay inside the list item
                lines = item_text.splitlines()
                indent = " " * (len(bullet) + 1)
                formatted = f"{bullet} {lines[0]}" if lines else f"{bullet}"
                for line in lines[1:]:
                    formatted += f"\n{indent}{line}"
                items.append(formatted)
            return "\n".join(items) + "\n"

        if node_type == "code":
            language = node.get("data", {}).get("syntax", "")
            code_text = GitBookMarkdownConverter._inline(children)
            return f"```{language}\n{code_text}\n```\n"

        if node_type == "hint":
            inner = GitBookMarkdownConverter._nodes(children)
            return "\n".join(f"> {line}" for line in inner.splitlines()) + "\n" if inner.strip() else ""

        if node_type == "divider":
            return "---\n"

        if node_type == "link":
            href = node.get("data", {}).get("url", "")
            inner = GitBookMarkdownConverter._inline(children)
            return f"[{inner}]({href})" if href else inner

        if node_type == "image":
            src = node.get("data", {}).get("src", "")
            alt = node.get("data", {}).get("alt", "")
            return f"![{alt}]({src})\n" if src else ""

        if node_type == "table":
            return GitBookMarkdownConverter._table(node)

        # Recurse into any unknown container nodes
        if children:
            return GitBookMarkdownConverter._nodes(children)

        return node.get("text", "")

    @staticmethod
    def _inline(nodes: list[dict]) -> str:
        return "".join(GitBookMarkdownConverter._node(n) for n in nodes)

    @staticmethod
    def _table(node: dict) -> str:
        rows = node.get("nodes", [])
        md_rows: list[list[str]] = []
        for row in rows:
            cells = [GitBookMarkdownConverter._inline(cell.get("nodes", [])) for cell in row.get("nodes", [])]
            md_rows.append(cells)
        if not md_rows:
            return ""
        header = "| " + " | ".join(md_rows[0]) + " |"
        separator = "| " + " | ".join("---" for _ in md_rows[0]) + " |"
        body = "\n".join("| " + " | ".join(r) + " |" for r in md_rows[1:])
        return "\n".join(filter(None, [header, separator, body])) + "\n"


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

        self._api_token = api_token
        self.space_ids: list[str] = parse_list(cfg.get("space_ids", []))

    def list_items(self) -> Iterator[IngestionItem]:
        client = GitBookClient(self._api_token)
        try:
            try:
                space_ids = self._resolve_space_ids(client)
            except Exception as exc:
                logger.error(f"[{self.source_name}] Failed to resolve space IDs: {exc}")
                return

            total = 0
            for space_id in space_ids:
                try:
                    pages = client.list_pages(space_id)
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
            client.close()

    def get_raw_content(self, item: IngestionItem) -> str:
        page = item.source_ref
        space_id = page.get("_space_id", "")
        page_id = page.get("id", "")
        title = page.get("title", "") or ""

        url = page.get("urls", {}).get("app", "") or ""
        item._metadata_cache["title"] = title
        item._metadata_cache["url"] = url

        client = GitBookClient(self._api_token)
        try:
            detail = client.get_page(space_id, page_id)
        except Exception as exc:
            logger.error(f"[{self.source_name}] Failed to fetch page {page_id!r} in space {space_id!r}: {exc}")
            return ""
        finally:
            client.close()

        document = detail.get("document") or {}
        content = GitBookMarkdownConverter.extract(document).strip()

        if not content:
            logger.warning(
                f"[{self.source_name}] Empty content for page {page_id!r} in space {space_id!r}. "
                "Ensure the API token has read access to this space."
            )
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
            "page_title": item._metadata_cache.get("title", page.get("title", "") or ""),
            "page_url": item._metadata_cache.get("url", ""),
            "parent_page_id": str(parent_id) if parent_id else "",
        }

    def _resolve_space_ids(self, client: GitBookClient) -> list[str]:
        """Return configured space IDs, or fetch all accessible ones if none configured."""
        if self.space_ids:
            return self.space_ids
        spaces = client.list_spaces()
        ids = [s["id"] for s in spaces if s.get("id")]
        logger.info(f"[{self.source_name}] Discovered {len(ids)} accessible space(s)")
        return ids
