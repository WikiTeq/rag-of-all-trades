import os
from unittest.mock import patch

import pytest

from utils.config import EnvSettings


def _make_env(**overrides):
    base = {
        "REDIS_URL": "redis://localhost:6379/0",
        "POSTGRES_USER": "u",
        "POSTGRES_PASSWORD": "p",
        "POSTGRES_DB": "db",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "DATABASE_URL": "postgresql://u:p@localhost/db",
        "OPENROUTER_API_KEY": "key",
        "OPENROUTER_API_BASE": "https://openrouter.ai/api/v1",
    }
    base.update(overrides)
    return base


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("  my-secret-key  ", "my-secret-key"),
        ("   ", ""),
        ("", ""),
    ],
)
def test_mcp_api_key_stripped(raw, expected):
    with patch.dict(os.environ, _make_env(MCP_API_KEY=raw), clear=True):
        settings = EnvSettings()
        assert settings.MCP_API_KEY == expected
