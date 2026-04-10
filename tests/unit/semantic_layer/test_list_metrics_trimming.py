from unittest.mock import MagicMock

from dbt_mcp.config.config_providers.base import SemanticLayerConfig
from dbt_mcp.config.settings import DbtMcpSettings


def test_sl_metrics_max_response_chars_default():
    settings = DbtMcpSettings(DBT_HOST=None, _env_file=None)
    assert settings.sl_metrics_max_response_chars == 16000


def test_sl_metrics_max_response_chars_from_env(monkeypatch):
    monkeypatch.setenv("DBT_MCP_SL_MAX_RESPONSE_CHARS", "8000")
    settings = DbtMcpSettings(DBT_HOST=None, _env_file=None)
    assert settings.sl_metrics_max_response_chars == 8000


def test_sl_metrics_max_response_chars_zero_allowed(monkeypatch):
    monkeypatch.setenv("DBT_MCP_SL_MAX_RESPONSE_CHARS", "0")
    settings = DbtMcpSettings(DBT_HOST=None, _env_file=None)
    assert settings.sl_metrics_max_response_chars == 0


def test_semantic_layer_config_max_response_chars_default():
    config = SemanticLayerConfig(
        url="https://example.com",
        host="example.com",
        prod_environment_id=1,
        token_provider=MagicMock(),
        headers_provider=MagicMock(),
    )
    assert config.max_response_chars == 16000


def test_semantic_layer_config_max_response_chars_custom():
    config = SemanticLayerConfig(
        url="https://example.com",
        host="example.com",
        prod_environment_id=1,
        token_provider=MagicMock(),
        headers_provider=MagicMock(),
        max_response_chars=8000,
    )
    assert config.max_response_chars == 8000
