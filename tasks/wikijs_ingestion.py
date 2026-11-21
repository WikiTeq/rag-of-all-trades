import logging
import re
from collections.abc import Iterator
from typing import Any

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.graphql import GraphQLError, graphql_request
from utils.http import RetrySession
from utils.parse import parse_bool, parse_list, parse_timestamp
from utils.text import slugify

logger = logging.getLogger(__name__)


class WikiJsClient:
    """GraphQL client for the Wiki.js API."""

    _PAGES_LIST_QUERY = """
query PageList($tags: [String!], $locale: String) {
  pages {
    list(tags: $tags, locale: $locale) {
      id
      path
      locale
      title
      createdAt
      updatedAt
      isPublished
      tags
    }
  }
}
"""

    _PAGE_SINGLE_QUERY = """
query PageSingle($id: Int!) {
  pages {
    single(id: $id) {
      id
      content
      contentType
    }
  }
}
"""

    _PAGE_RENDER_QUERY = """
query PageRender($id: Int!) {
  pages {
    single(id: $id) {
      render
    }
  }
}
"""

    def __init__(self, base_url: str, api_token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        }
        self._session = RetrySession()

    def list_pages(self, tags: list[str] | None = None, locale: str | None = None) -> list[dict]:
        variables: dict[str, Any] = {}
        if tags:
            variables["tags"] = tags
        if locale:
            variables["locale"] = locale
        data = graphql_request(
            f"{self.base_url}/graphql",
            self._PAGES_LIST_QUERY,
            variables,
            self._headers,
            session=self._session,
        )
        pages = data.get("pages", {}).get("list", [])
        if not isinstance(pages, list):
            logger.error(f"Unexpected pages.list response shape: {type(pages)}")
            return []
        return pages

    def get_page(self, page_id: int) -> dict:
        return self._get_single(self._PAGE_SINGLE_QUERY, page_id)

    def get_page_render(self, page_id: int) -> str:
        """Return the HTML Wiki.js renders for a page (used for asciidoc pages)."""
        return self._get_single(self._PAGE_RENDER_QUERY, page_id).get("render") or ""

    def _get_single(self, query: str, page_id: int) -> dict:
        data = graphql_request(
            f"{self.base_url}/graphql",
            query,
            {"id": page_id},
            self._headers,
            session=self._session,
        )
        page = data.get("pages", {}).get("single", {})
        if not isinstance(page, dict):
            logger.error(f"Unexpected pages.single response shape for id={page_id}: {type(page)}")
            return {}
        return page


class WikiJsIngestionJob(IngestionJob):
    """Ingestion connector for Wiki.js instances.

    Fetches pages via the Wiki.js GraphQL API and stores them in the vector store.
    Only Wiki.js v2 is supported: v3 replaces the GraphQL API with a REST API.

    Configuration (config.yaml):
        - config.base_url: Wiki.js instance base URL (required)
        - config.api_token: API token (required). Needs the read:pages, read:source and
          manage:pages (or delete:pages) scopes. See
          https://github.com/requarks/wiki/discussions/8065 for why `pages.single` needs
          manage:pages (or delete:pages).
        - config.paths: path prefixes to ingest; if omitted, all pages are ingested
          (optional, comma-separated string or list)
        - config.tags: server-side tag filter (optional, comma-separated string or list)
        - config.locale: server-side locale filter, e.g. "en" (optional)
        - config.include_unpublished: whether to ingest unpublished pages (optional, default false)
    """

    # manage:pages (or delete:pages) is needed on top of read:pages and read:source because
    # of how Wiki.js v2 checks permissions in `pages.single`; see
    # https://github.com/requarks/wiki/discussions/8065
    # Wiki.js adds a pilcrow anchor link to every rendered heading, e.g. "[¶](#section)"
    _HEADING_ANCHOR = re.compile(r"\[¶\]\(#[^)]*\)\s*")

    _SCOPES_HINT = "The API token needs the read:pages, read:source and manage:pages (or delete:pages) scopes."

    @property
    def source_type(self) -> str:
        return "wikijs"

    def __init__(self, config: dict):
        super().__init__(config)

        cfg = config.get("config", {})

        base_url = cfg.get("base_url", "").rstrip("/")
        if not base_url:
            raise ValueError("base_url is required in Wiki.js connector config")

        api_token = cfg.get("api_token", "").strip()
        if not api_token:
            raise ValueError("api_token is required in Wiki.js connector config")

        # Normalize path prefixes: ensure leading slash, no trailing slash
        raw_paths = parse_list(cfg.get("paths", []))
        self.paths = ["/" + p.strip("/") for p in raw_paths if p.strip("/")]
        self.tags = parse_list(cfg.get("tags", []))
        self.locale = cfg.get("locale", "").strip() or None
        self.include_unpublished = parse_bool(cfg.get("include_unpublished", False))

        self._client = WikiJsClient(base_url, api_token)

    def get_item_checksum(self, item: IngestionItem) -> str | None:
        updated_at = item.source_ref.get("updatedAt", "")
        return f"wikijs:{item.source_ref.get('id')}:{updated_at}" if updated_at else None

    def list_items(self) -> Iterator[IngestionItem]:
        logger.info(f"[{self.source_name}] Listing Wiki.js pages")

        try:
            pages = self._client.list_pages(tags=self.tags, locale=self.locale)
        except GraphQLError as e:
            logger.error(f"[{self.source_name}] Failed to list pages: {e}")
            return
        except Exception:
            logger.exception(f"[{self.source_name}] Unexpected error listing pages")
            raise

        count = 0
        for page in pages:
            page_id = page.get("id")
            if page_id is None:
                logger.warning(f"[{self.source_name}] Skipping page with missing id: {page}")
                continue

            if not self.include_unpublished and not page.get("isPublished", True):
                continue

            path = page.get("path", "") or ""
            norm_path = "/" + path.lstrip("/")
            if self.paths and not any(norm_path == p or norm_path.startswith(p + "/") for p in self.paths):
                continue

            updated_at = parse_timestamp(page.get("updatedAt"))
            yield IngestionItem(
                id=f"wikijs:{page_id}",
                source_ref=page,
                last_modified=updated_at,
            )
            count += 1

        logger.info(f"[{self.source_name}] Found {count} page(s)")

    def get_raw_content(self, item: IngestionItem) -> str:
        page_id = item.source_ref.get("id")
        try:
            detail = self._client.get_page(page_id)
        except GraphQLError as e:
            logger.error(f"[{self.source_name}] Cannot read page {page_id} ({e}). {self._SCOPES_HINT}")
            raise

        raw = detail.get("content", "") or ""
        if not raw.strip():
            path = item.source_ref.get("path", "")
            logger.warning(f"[{self.source_name}] Empty content for page {page_id} ({path!r}). {self._SCOPES_HINT}")
            return ""

        content_type = detail.get("contentType", "").lower()
        if content_type == "html":
            content = self.convert_to_markdown(raw, file_extension=".html")
        elif content_type == "asciidoc":
            content = self._asciidoc_to_markdown(page_id)
            if not content:
                return ""
        else:
            content = raw.strip()

        # Don't prepend title if content already starts with it
        title = item.source_ref.get("title", "") or ""
        first_line = content.split("\n", 1)[0].lstrip("# ").strip()
        if title and first_line.lower() != title.lower():
            return f"# {title}\n\n{content}"
        return content

    def _asciidoc_to_markdown(self, page_id: int) -> str:
        """Convert an asciidoc page to Markdown through the HTML that Wiki.js renders.

        MarkItDown has no asciidoc converter, so the rendered HTML is converted instead.
        Returns an empty string when Wiki.js returns no render or the conversion fails.
        """
        rendered = self._client.get_page_render(page_id)
        if not rendered.strip():
            logger.warning(f"[{self.source_name}] No rendered HTML for asciidoc page {page_id}, skipping")
            return ""
        # Pass bytes: for str input convert_to_markdown falls back to the original HTML on failure
        converted = self.convert_to_markdown(rendered.encode("utf-8"), file_extension=".html")
        content = self._HEADING_ANCHOR.sub("", converted).strip()
        if not content:
            logger.warning(f"[{self.source_name}] Could not convert asciidoc page {page_id} to Markdown")
        return content

    def get_item_name(self, item: IngestionItem) -> str:
        title = item.source_ref.get("title", "") or f"wikijs-{item.source_ref.get('id', '')}"
        return slugify(title, max_len=255)

    def get_extra_metadata(self, item: IngestionItem, content: str, metadata: dict[str, Any]) -> dict[str, Any]:
        data = item.source_ref
        raw_tags = data.get("tags") or []
        tags = [t for t in raw_tags if isinstance(t, str) and t]
        return {
            "page_id": str(data.get("id", "")),
            "path": data.get("path", "") or "",
            "locale": data.get("locale", "") or "",
            "title": data.get("title", "") or "",
            "url": f"{self._client.base_url}/{data.get('locale', '')}/{(data.get('path', '') or '').lstrip('/')}",
            "created_at": str(data.get("createdAt", "") or ""),
            "updated_at": str(data.get("updatedAt", "") or ""),
            "tags": ",".join(tags),
            "is_published": bool(data.get("isPublished", True)),
        }
