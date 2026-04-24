import logging
from datetime import UTC, datetime

from tasks.base import IngestionJob
from tasks.helper_classes.ingestion_item import IngestionItem
from utils.http import RetrySession
from utils.parse import parse_list
from utils.text import slugify

logger = logging.getLogger(__name__)


class SerpAPIIngestionJob(IngestionJob):
    @property
    def source_type(self) -> str:
        return "serpapi"

    def __init__(self, config):
        super().__init__(config)

        cfg = config.get("config", {})

        self.api_key = cfg.get("api_key")

        queries = parse_list(cfg.get("queries"))

        if not queries:
            raise ValueError(f"[{config.get('name')}] SerpAPI connector requires at least one query")

        self.search_queries = queries
        self.serpapi_endpoint = "https://serpapi.com/search"
        self._session = RetrySession()

    def list_items(self):
        for query in self.search_queries:
            yield IngestionItem(
                id=f"serpapi:{query}",
                source_ref=query,
                last_modified=datetime.now(UTC),
            )

    def get_raw_content(self, item: IngestionItem) -> str:
        query: str = item.source_ref
        try:
            params = {
                "engine": "google",
                "q": query,
                "api_key": self.api_key,
            }

            resp = self._session.get(self.serpapi_endpoint, params=params)
            resp.raise_for_status()

            data = resp.json()

            # SerpAPI returns a rich JSON response; we extract only titles and snippets
            # from organic_results as a lightweight plain-text representation
            titles = [r.get("title") for r in data.get("organic_results", []) if r.get("title")]
            snippets = [r.get("snippet") for r in data.get("organic_results", []) if r.get("snippet")]

            text_content = "\n".join(titles + snippets).strip()
            return text_content

        except Exception as e:
            logger.info(f"[SerpAPI] Failed to fetch query '{query}': {e}")
            return ""

    def get_item_name(self, item: IngestionItem) -> str:
        return slugify(str(item.source_ref), max_len=255)
