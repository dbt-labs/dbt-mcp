from unittest.mock import Mock, patch

import pytest

from dbt_mcp.discovery.client import SourcesFetcher, MetadataAPIClient
from dbt_mcp.errors import GraphQLError


@pytest.fixture
def mock_api_client():
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
def sources_fetcher(mock_api_client):
    return SourcesFetcher(api_client=mock_api_client)


async def test_fetch_sources_single_page(sources_fetcher, mock_api_client):
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "sources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "cursor_end"},
                        "edges": [
                            {
                                "node": {
                                    "name": "customers",
                                    "uniqueId": "source.test_project.raw_data.customers",
                                    "description": "Customer data from external system",
                                    "sourceName": "raw_data",
                                    "resourceType": "source",
                                    "freshness": {
                                        "maxLoadedAt": "2024-01-15T10:30:00Z",
                                        "maxLoadedAtTimeAgoInS": 3600,
                                        "freshnessStatus": "pass"
                                    }
                                }
                            },
                            {
                                "node": {
                                    "name": "orders",
                                    "uniqueId": "source.test_project.raw_data.orders",
                                    "description": "Order data from external system",
                                    "sourceName": "raw_data",
                                    "resourceType": "source",
                                    "freshness": {
                                        "maxLoadedAt": "2024-01-15T11:00:00Z",
                                        "maxLoadedAtTimeAgoInS": 1800,
                                        "freshnessStatus": "warn"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
    }

    # Set up the mock to return our response
    mock_api_client.execute_query.return_value = mock_response

    # Execute the fetch
    result = await sources_fetcher.fetch_sources()

    # Verify the API was called correctly
    mock_api_client.execute_query.assert_called_once()
    call_args = mock_api_client.execute_query.call_args
    
    # Check that the GraphQL query contains expected elements
    query = call_args[0][0]
    assert "GetSources" in query
    assert "environment" in query
    assert "applied" in query
    assert "sources" in query
    
    # Check variables
    variables = call_args[0][1]
    assert variables["environmentId"] == 123
    assert variables["first"] == 100  # PAGE_SIZE
    assert variables["sourcesFilter"] == {}

    # Verify the result
    assert len(result) == 2
    assert result[0]["name"] == "customers"
    assert result[0]["sourceName"] == "raw_data"
    assert result[0]["resourceType"] == "source"
    assert result[0]["freshness"]["freshnessStatus"] == "pass"
    assert result[1]["name"] == "orders"
    assert result[1]["freshness"]["freshnessStatus"] == "warn"


async def test_fetch_sources_with_filter(sources_fetcher, mock_api_client):
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "sources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "cursor_end"},
                        "edges": [
                            {
                                "node": {
                                    "name": "customers",
                                    "uniqueId": "source.test_project.external_api.customers",
                                    "description": "Customer data from API",
                                    "sourceName": "external_api",
                                    "resourceType": "source",
                                    "freshness": {
                                        "maxLoadedAt": "2024-01-15T10:30:00Z",
                                        "maxLoadedAtTimeAgoInS": 3600,
                                        "freshnessStatus": "pass"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    # Execute with filter
    source_filter = {"sourceNames": ["external_api"]}
    result = await sources_fetcher.fetch_sources(source_filter=source_filter)

    # Verify the filter was passed correctly
    call_args = mock_api_client.execute_query.call_args
    variables = call_args[0][1]
    assert variables["sourcesFilter"] == source_filter

    # Verify the result
    assert len(result) == 1
    assert result[0]["sourceName"] == "external_api"


async def test_fetch_sources_empty_response(sources_fetcher, mock_api_client):
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "sources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "edges": []
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    result = await sources_fetcher.fetch_sources()

    assert result == []


async def test_fetch_sources_pagination(sources_fetcher, mock_api_client):
    # First page response
    first_page_response = {
        "data": {
            "environment": {
                "applied": {
                    "sources": {
                        "pageInfo": {"hasNextPage": True, "endCursor": "cursor_page_1"},
                        "edges": [
                            {
                                "node": {
                                    "name": "customers",
                                    "uniqueId": "source.test_project.raw_data.customers",
                                    "description": "Customer data",
                                    "sourceName": "raw_data",
                                    "resourceType": "source",
                                    "freshness": {
                                        "maxLoadedAt": "2024-01-15T10:30:00Z",
                                        "maxLoadedAtTimeAgoInS": 3600,
                                        "freshnessStatus": "pass"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
    }

    # Second page response (same cursor to stop pagination)
    second_page_response = {
        "data": {
            "environment": {
                "applied": {
                    "sources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "cursor_page_1"},  # hasNextPage False stops pagination
                        "edges": [
                            {
                                "node": {
                                    "name": "orders",
                                    "uniqueId": "source.test_project.raw_data.orders",
                                    "description": "Order data",
                                    "sourceName": "raw_data",
                                    "resourceType": "source",
                                    "freshness": {
                                        "maxLoadedAt": "2024-01-15T11:00:00Z",
                                        "maxLoadedAtTimeAgoInS": 1800,
                                        "freshnessStatus": "warn"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
    }

    # Set up mock to return different responses for each call
    mock_api_client.execute_query.side_effect = [first_page_response, second_page_response]

    result = await sources_fetcher.fetch_sources()

    # Should have called twice due to pagination
    assert mock_api_client.execute_query.call_count == 2
    
    # Check that the second call includes the cursor from the first response
    first_call_args = mock_api_client.execute_query.call_args_list[0]
    second_call_args = mock_api_client.execute_query.call_args_list[1]
    
    # First call should have empty after cursor
    assert first_call_args[0][1]["after"] == ""
    
    # Second call should have the cursor from first response
    assert second_call_args[0][1]["after"] == "cursor_page_1"
    
    # Should have both results
    assert len(result) == 2
    assert result[0]["name"] == "customers"
    assert result[1]["name"] == "orders"


@patch("dbt_mcp.discovery.client.raise_gql_error")
async def test_fetch_sources_graphql_error_handling(mock_raise_gql_error, sources_fetcher, mock_api_client):
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "sources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "edges": []
                    }
                }
            }
        }
    }

    # Configure the mock to raise GraphQLError when called
    mock_raise_gql_error.side_effect = GraphQLError("Test GraphQL error")
    
    mock_api_client.execute_query.return_value = mock_response

    # Verify that fetch_sources raises GraphQLError
    with pytest.raises(GraphQLError, match="Test GraphQL error"):
        await sources_fetcher.fetch_sources()

    # Verify that error handling function was called
    mock_raise_gql_error.assert_called_with(mock_response)


async def test_get_environment_id(sources_fetcher):
    environment_id = await sources_fetcher.get_environment_id()
    assert environment_id == 123


async def test_fetch_sources_with_unique_ids_filter(sources_fetcher, mock_api_client):
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "sources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "cursor_end"},
                        "edges": [
                            {
                                "node": {
                                    "name": "customers",
                                    "uniqueId": "source.test_project.raw_data.customers",
                                    "description": "Customer data",
                                    "sourceName": "raw_data",
                                    "resourceType": "source",
                                    "freshness": {
                                        "maxLoadedAt": "2024-01-15T10:30:00Z",
                                        "maxLoadedAtTimeAgoInS": 3600,
                                        "freshnessStatus": "pass"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    # Execute with uniqueIds filter
    source_filter = {"uniqueIds": ["source.test_project.raw_data.customers"]}
    result = await sources_fetcher.fetch_sources(source_filter=source_filter)

    # Verify the filter was passed correctly
    call_args = mock_api_client.execute_query.call_args
    variables = call_args[0][1]
    assert variables["sourcesFilter"] == source_filter

    # Verify the result
    assert len(result) == 1
    assert result[0]["uniqueId"] == "source.test_project.raw_data.customers"


async def test_fetch_sources_with_tags_filter(sources_fetcher, mock_api_client):
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "sources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "cursor_end"},
                        "edges": [
                            {
                                "node": {
                                    "name": "events",
                                    "uniqueId": "source.test_project.analytics.events",
                                    "description": "Analytics events data",
                                    "sourceName": "analytics",
                                    "resourceType": "source",
                                    "freshness": {
                                        "maxLoadedAt": "2024-01-15T09:00:00Z",
                                        "maxLoadedAtTimeAgoInS": 7200,
                                        "freshnessStatus": "warn"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    # Execute with tags filter
    source_filter = {"tags": ["analytics", "daily"]}
    result = await sources_fetcher.fetch_sources(source_filter=source_filter)

    # Verify the filter was passed correctly
    call_args = mock_api_client.execute_query.call_args
    variables = call_args[0][1]
    assert variables["sourcesFilter"] == source_filter

    # Verify the result
    assert len(result) == 1
    assert result[0]["sourceName"] == "analytics"


async def test_fetch_sources_with_combined_filters(sources_fetcher, mock_api_client):
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "sources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "cursor_end"},
                        "edges": [
                            {
                                "node": {
                                    "name": "customers",
                                    "uniqueId": "source.test_project.core.customers",
                                    "description": "Core customer data with freshness check",
                                    "sourceName": "core",
                                    "resourceType": "source",
                                    "freshness": {
                                        "maxLoadedAt": "2024-01-15T12:00:00Z",
                                        "maxLoadedAtTimeAgoInS": 600,
                                        "freshnessStatus": "pass"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    # Execute with combined filters
    source_filter = {
        "sourceNames": ["core"],
        "database": "production",
        "freshnessStatus": "pass",
        "tags": ["core", "customer"]
    }
    result = await sources_fetcher.fetch_sources(source_filter=source_filter)

    # Verify the filter was passed correctly
    call_args = mock_api_client.execute_query.call_args
    variables = call_args[0][1]
    assert variables["sourcesFilter"] == source_filter

    # Verify the result
    assert len(result) == 1
    assert result[0]["sourceName"] == "core"


async def test_fetch_sources_with_new_schema_filters(sources_fetcher, mock_api_client):
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "sources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "cursor_end"},
                        "edges": [
                            {
                                "node": {
                                    "name": "customers",
                                    "uniqueId": "source.test_project.analytics.customers",
                                    "description": "Fresh customer data",
                                    "sourceName": "analytics",
                                    "resourceType": "source",
                                    "freshness": {
                                        "maxLoadedAt": "2024-01-15T10:30:00Z",
                                        "maxLoadedAtTimeAgoInS": 3600,
                                        "freshnessStatus": "pass"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
    }

    mock_api_client.execute_query.return_value = mock_response

    # Execute with new schema filters
    source_filter = {
        "sourceNames": ["analytics", "staging"],
        "schema": "prod", 
        "freshnessStatus": "pass",
        "tags": ["daily"]
    }
    result = await sources_fetcher.fetch_sources(source_filter=source_filter)

    # Verify the filter was passed correctly
    call_args = mock_api_client.execute_query.call_args
    variables = call_args[0][1]
    assert variables["sourcesFilter"] == source_filter

    # Verify the result
    assert len(result) == 1
    assert result[0]["sourceName"] == "analytics"