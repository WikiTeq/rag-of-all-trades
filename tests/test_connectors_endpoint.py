import unittest
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1.connectors.routes import router as connectors_router
from utils.config import Settings
from utils.connectors import build_connector_list


def _patch_sources(sources):
    return patch.object(Settings, "SOURCES", property(lambda self: sources))


class BuildConnectorListTests(unittest.TestCase):
    def test_excludes_disabled_sources(self):
        sources = [
            {"type": "s3", "name": "a", "enabled": True, "config": {"endpoint": "https://s3.example.com"}},
            {"type": "s3", "name": "b", "enabled": False, "config": {"endpoint": "https://s3-disabled.example.com"}},
        ]
        with _patch_sources(sources):
            result = build_connector_list()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "a")

    def test_excludes_sensitive_fields(self):
        sources = [
            {
                "type": "jira",
                "name": "jira1",
                "enabled": True,
                "config": {
                    "server_url": "https://jira.example.com",
                    "jql": "project = X",
                    "auth_type": "basic",
                    "email": "user@example.com",
                    "api_token": "super-secret",
                },
            }
        ]
        with _patch_sources(sources):
            result = build_connector_list()

        config = result[0]["config"]
        self.assertEqual(config["server_url"], "https://jira.example.com")
        self.assertNotIn("email", config)
        self.assertNotIn("api_token", config)

    def test_missing_optional_fields_are_omitted_without_error(self):
        sources = [
            {"type": "web", "name": "web1", "enabled": True, "config": {"urls": ["https://example.com"]}},
        ]
        with _patch_sources(sources):
            result = build_connector_list()

        config = result[0]["config"]
        self.assertEqual(config["urls"], ["https://example.com"])
        self.assertNotIn("sitemap_url", config)

    def test_unknown_type_returns_empty_config(self):
        sources = [{"type": "unknown", "name": "x", "enabled": True, "config": {"secret": "value"}}]
        with _patch_sources(sources):
            result = build_connector_list()

        self.assertEqual(result[0]["config"], {})

    def test_enabled_defaults_true_when_missing(self):
        sources = [{"type": "serpapi", "name": "serp1", "config": {"queries": ["foo"]}}]
        with _patch_sources(sources):
            result = build_connector_list()

        self.assertEqual(len(result), 1)

    def test_common_field_surfaced_regardless_of_type(self):
        sources = [
            {"type": "serpapi", "name": "serp1", "enabled": True, "config": {"queries": ["foo"], "request_delay": 2}}
        ]
        with _patch_sources(sources):
            result = build_connector_list()

        self.assertEqual(result[0]["config"]["request_delay"], 2)


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(connectors_router)
    return app


class ConnectorsRouteTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(_make_app())

    def test_returns_connector_list_without_auth_configured(self):
        sources = [{"type": "s3", "name": "a", "enabled": True, "config": {"endpoint": "https://s3.example.com"}}]
        with (
            patch("api.dependencies.settings.env.API_KEY", ""),
            _patch_sources(sources),
        ):
            response = self.client.get("/connectors")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(len(body["connectors"]), 1)
        self.assertEqual(body["connectors"][0]["type"], "s3")

    def test_requires_api_key_when_configured(self):
        with patch("api.dependencies.settings.env.API_KEY", "secret"):
            response = self.client.get("/connectors")

        self.assertEqual(response.status_code, 401)


if __name__ == "__main__":
    unittest.main()
