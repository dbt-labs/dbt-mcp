from unittest.mock import Mock

import pytest

from dbt_mcp.discovery.client import MetadataAPIClient


@pytest.fixture
def mock_api_client():
    """
    Shared mock MetadataAPIClient for discovery tests.

    Provides a mock API client with:
    - A config_provider that returns environment_id = 123
    - An async get_config() method for compatibility with async tests

    Used by test_sources_fetcher.py and test_exposures_fetcher.py.
    """
    mock_client = Mock(spec=MetadataAPIClient)
    # Add config_provider mock that returns environment_id
    mock_config_provider = Mock()
    mock_config = Mock()
    mock_config.environment_id = 123

    # Make get_config async
    async def mock_get_config():
        return mock_config

    mock_config_provider.get_config = mock_get_config
    mock_client.config_provider = mock_config_provider
    return mock_client
