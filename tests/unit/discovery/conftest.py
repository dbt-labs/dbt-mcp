from unittest.mock import AsyncMock, Mock

import pytest

from dbt_mcp.discovery.client import LineageFetcher, MetadataAPIClient
from dbt_mcp.discovery.tools import DiscoveryToolContext


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


@pytest.fixture
def lineage_fetcher(mock_api_client):
    """Shared LineageFetcher instance with mocked API client."""
    return LineageFetcher(api_client=mock_api_client)


@pytest.fixture
def mock_discovery_context():
    """Shared mock DiscoveryToolContext for tool testing."""
    context = Mock(spec=DiscoveryToolContext)
    context.lineage_fetcher = Mock()
    context.lineage_fetcher.search_all_resources = AsyncMock()
    context.lineage_fetcher.fetch_lineage = AsyncMock()
    return context


@pytest.fixture
def mock_mcp_context():
    """Shared mock MCP Context for elicitation testing."""
    ctx = Mock()
    ctx.elicit = AsyncMock()
    return ctx
