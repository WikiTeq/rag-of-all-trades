from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.dependencies import require_api_key


def _make_app() -> FastAPI:
    from fastapi import Depends

    app = FastAPI()

    @app.get("/protected")
    def protected(_auth: None = Depends(require_api_key)):
        return {"ok": True}

    return app


@pytest.fixture()
def client():
    return TestClient(_make_app(), raise_server_exceptions=True)


def test_no_api_key_configured_request_without_header_passes(client):
    with patch("api.dependencies.settings.env.API_KEY", ""):
        response = client.get("/protected")
    assert response.status_code == 200


def test_no_api_key_configured_request_with_header_passes(client):
    with patch("api.dependencies.settings.env.API_KEY", ""):
        response = client.get("/protected", headers={"Authorization": "Bearer anything"})
    assert response.status_code == 200


def test_api_key_configured_correct_token_passes(client):
    with patch("api.dependencies.settings.env.API_KEY", "secret"):
        response = client.get("/protected", headers={"Authorization": "Bearer secret"})
    assert response.status_code == 200


def test_api_key_configured_wrong_token_returns_401(client):
    with patch("api.dependencies.settings.env.API_KEY", "secret"):
        response = client.get("/protected", headers={"Authorization": "Bearer wrong"})
    assert response.status_code == 401


def test_api_key_configured_missing_header_returns_401(client):
    with patch("api.dependencies.settings.env.API_KEY", "secret"):
        response = client.get("/protected")
    assert response.status_code == 401
