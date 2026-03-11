import logging

import requests

from tasks.base import IngestionJob

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class SerpAPIIngestionJob(IngestionJob):
    @property
    def source_type(self) -> str:
        return "serpapi"

    def __init__(self, config):
        super().__init__(config)

        cfg = config.get("config", {})

        self.api_key = cfg.get("api_key")

        queries = cfg.get("queries", [])
        if isinstance(queries, str):
            queries = [q.strip() for q in queries.split(",") if q.strip()]

        if not queries:
            raise ValueError(
                f"[{config.get('name')}] SerpAPI connector requires at least one query"
            )

        self.search_queries = queries

        self.serpapi_endpoint = "https://serpapi.com/search"

    def list_items(self):
        return self.search_queries

    def get_raw_content(self, query: str) -> str:
        try:
            params = {
                "engine": "google",
                "q": query,
                "api_key": self.api_key,
            }

            resp = requests.get(self.serpapi_endpoint, params=params)
            resp.raise_for_status()

            data = resp.json()

            # SerpAPI returns a rich JSON response; we extract only titles and snippets
            # from organic_results as a lightweight plain-text representation
            titles = [
                r.get("title")
                for r in data.get("organic_results", [])
                if r.get("title")
            ]
            snippets = [
                r.get("snippet")
                for r in data.get("organic_results", [])
                if r.get("snippet")
            ]

            text_content = "\n".join(titles + snippets).strip()
            return text_content

        except Exception as e:
            logger.info(f"[SerpAPI] Failed to fetch query '{query}': {e}")
            return ""

    def get_item_name(self, query: str) -> str:
        return query
