from unittest.mock import Mock

import pytest

from dbt_mcp.discovery.client import MetadataAPIClient


@pytest.fixture
def mock_config_provider():
    """
    Shared mock config provider for discovery tests.

    Provides:
    - environment_id = 123
    - Async get_config() method

    Used by test_get_lineage_tool.py and embedded in mock_api_client.
    """
    mock_provider = Mock()
    mock_config = Mock()
    mock_config.environment_id = 123

    async def mock_get_config():
        return mock_config

    mock_provider.get_config = mock_get_config
    return mock_provider


@pytest.fixture
def mock_api_client(mock_config_provider):
    """
    Shared mock MetadataAPIClient for discovery tests.

    Provides a mock API client with:
    - Mocked execute_query() method
    - Integrated config_provider with environment_id = 123
    - Async compatible

    Used by test_lineage_fetcher.py, test_sources_fetcher.py, and test_exposures_fetcher.py.
    """
    mock_client = Mock(spec=MetadataAPIClient)
    mock_client.config_provider = mock_config_provider
    return mock_client


@pytest.fixture
def response_builders():
    """
    Factory for creating standardized GraphQL responses.

    Provides builder methods for common response patterns:
    - model_search_response: Build model search responses
    - source_search_response: Build source search responses
    - lineage_response: Build lineage responses

    Used by test_lineage_fetcher.py and test_get_lineage_tool.py.
    """

    class ResponseBuilders:
        @staticmethod
        def model_search_response(models=None, end_cursor=None):
            """
            Build a model search response.

            Args:
                models: List of dicts with keys: name, uniqueId, description
                end_cursor: Pagination cursor (default: None)

            Example:
                response_builders.model_search_response(
                    models=[{"name": "customers", "uniqueId": "model.x.customers", "description": "..."}]
                )
            """
            if models is None:
                models = []

            edges = [{"node": model} for model in models]

            return {
                "data": {
                    "environment": {
                        "applied": {
                            "models": {
                                "pageInfo": {"endCursor": end_cursor},
                                "edges": edges
                            }
                        }
                    }
                }
            }

        @staticmethod
        def source_search_response(sources=None, end_cursor=None, has_next_page=False):
            """
            Build a source search response.

            Args:
                sources: List of dicts with keys: name, uniqueId, description
                end_cursor: Pagination cursor
                has_next_page: Whether more results exist

            Example:
                response_builders.source_search_response(
                    sources=[{"name": "customers", "uniqueId": "source.x.raw.customers", "description": "..."}]
                )
            """
            if sources is None:
                sources = []

            edges = [{"node": source} for source in sources]

            return {
                "data": {
                    "environment": {
                        "applied": {
                            "sources": {
                                "pageInfo": {"hasNextPage": has_next_page, "endCursor": end_cursor},
                                "edges": edges
                            }
                        }
                    }
                }
            }

        @staticmethod
        def lineage_response(nodes=None):
            """
            Build a lineage response.

            Args:
                nodes: List of lineage node dicts with keys:
                       uniqueId, name, resourceType, matchesMethod, filePath (optional)

            Example:
                response_builders.lineage_response(
                    nodes=[{
                        "uniqueId": "model.x.customers",
                        "name": "customers",
                        "resourceType": "Model",
                        "matchesMethod": True
                    }]
                )
            """
            if nodes is None:
                nodes = []

            return {
                "data": {
                    "environment": {
                        "applied": {
                            "lineage": nodes
                        }
                    }
                }
            }

    return ResponseBuilders()
