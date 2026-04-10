from unittest.mock import MagicMock

from dbt_mcp.config.config_providers.base import SemanticLayerConfig


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
