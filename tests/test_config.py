import os
from unittest.mock import patch

import pytest

from utils.config import DEFAULT_SIMILARITY_CUTOFF, EnvSettings, Settings


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


def _settings_with_yaml(yaml_data):
    settings = Settings.__new__(Settings)
    settings.yaml = yaml_data
    return settings


@pytest.mark.parametrize(
    "yaml_data,expected",
    [
        ({}, DEFAULT_SIMILARITY_CUTOFF),
        ({"vector_store": {}}, DEFAULT_SIMILARITY_CUTOFF),
        ({"vector_store": {"similarity_cutoff": None}}, DEFAULT_SIMILARITY_CUTOFF),
        ({"vector_store": {"similarity_cutoff": 0.35}}, 0.35),
        ({"vector_store": {"similarity_cutoff": 0}}, 0.0),
    ],
)
def test_similarity_cutoff_from_yaml(yaml_data, expected):
    assert _settings_with_yaml(yaml_data).SIMILARITY_CUTOFF == expected


@pytest.mark.parametrize("value", [-0.1, 1.5])
def test_similarity_cutoff_out_of_range_raises(value):
    with pytest.raises(ValueError):
        _settings_with_yaml({"vector_store": {"similarity_cutoff": value}}).SIMILARITY_CUTOFF
