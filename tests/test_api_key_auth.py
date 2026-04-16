import unittest
from unittest.mock import patch

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from api.dependencies import require_api_key


def _make_app() -> FastAPI:
    app = FastAPI()

    @app.get("/protected")
    def protected(_auth: None = Depends(require_api_key)):
        return {"ok": True}

    return app


class ApiKeyAuthTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(_make_app(), raise_server_exceptions=True)

    def test_no_api_key_configured_request_without_header_passes(self):
        with patch("api.dependencies.settings.env.API_KEY", ""):
            response = self.client.get("/protected")
        self.assertEqual(response.status_code, 200)

    def test_no_api_key_configured_request_with_header_passes(self):
        with patch("api.dependencies.settings.env.API_KEY", ""):
            response = self.client.get("/protected", headers={"Authorization": "Bearer anything"})
        self.assertEqual(response.status_code, 200)

    def test_api_key_configured_correct_token_passes(self):
        with patch("api.dependencies.settings.env.API_KEY", "secret"):
            response = self.client.get("/protected", headers={"Authorization": "Bearer secret"})
        self.assertEqual(response.status_code, 200)

    def test_api_key_configured_wrong_token_returns_401(self):
        with patch("api.dependencies.settings.env.API_KEY", "secret"):
            response = self.client.get("/protected", headers={"Authorization": "Bearer wrong"})
        self.assertEqual(response.status_code, 401)

    def test_api_key_configured_missing_header_returns_401(self):
        with patch("api.dependencies.settings.env.API_KEY", "secret"):
            response = self.client.get("/protected")
        self.assertEqual(response.status_code, 401)
