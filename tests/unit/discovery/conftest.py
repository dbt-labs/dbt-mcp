from unittest.mock import Mock

import pytest

from dbt_mcp.config.config_providers import DiscoveryConfig
from dbt_mcp.discovery.client import MetadataAPIClient


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
def mock_api_client(unit_discovery_config: DiscoveryConfig):
    """
    Shared mock MetadataAPIClient for discovery tests.

    Provides a mock API client with:
    - A config_provider that returns unit_discovery_config
    - An async get_config() method for compatibility with async tests

    Used by test_sources_fetcher.py and test_exposures_fetcher.py.
    """
    mock_client = Mock(spec=MetadataAPIClient)
    mock_config_provider = Mock()

    async def mock_get_config():
        return unit_discovery_config

    mock_config_provider.get_config = mock_get_config
    mock_client.config_provider = mock_config_provider
    return mock_client
