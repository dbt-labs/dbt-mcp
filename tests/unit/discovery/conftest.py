from unittest.mock import AsyncMock, Mock, patch

import pytest

from dbt_mcp.config.config_providers import DiscoveryConfig


@pytest.fixture
def unit_discovery_config() -> DiscoveryConfig:
    headers = Mock()
    headers.get_headers.return_value = {}
    return DiscoveryConfig(
        url="https://metadata.example.com/graphql",
        headers_provider=headers,
        environment_id=123,
    )


@pytest.fixture
def mock_api_client():
    """
    Patches the module-level execute_query in dbt_mcp.discovery.client with an AsyncMock.

    Tests can use mock_api_client.return_value, mock_api_client.side_effect, etc.
    to control the return values of the patched execute_query function.
    """
    mock_execute_query = AsyncMock()
    with patch("dbt_mcp.discovery.client.execute_query", mock_execute_query):
        yield mock_execute_query
