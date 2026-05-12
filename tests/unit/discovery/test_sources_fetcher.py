from unittest.mock import patch

import pytest

from dbt_mcp.discovery.client import (
    DEFAULT_MAX_NODE_QUERY_LIMIT,
    DEFAULT_PAGE_SIZE,
    PaginatedResourceFetcher,
    SourcesFetcher,
)
from dbt_mcp.errors import GraphQLError


@pytest.fixture
def sources_fetcher():
    paginator = PaginatedResourceFetcher(
        edges_path=("data", "environment", "applied", "sources", "edges"),
        page_info_path=("data", "environment", "applied", "sources", "pageInfo"),
        page_size=DEFAULT_PAGE_SIZE,
        max_node_query_limit=DEFAULT_MAX_NODE_QUERY_LIMIT,
    )
    return SourcesFetcher(paginator=paginator)


async def test_fetch_sources_single_page(
    sources_fetcher, mock_api_client, unit_discovery_config
):
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
                                        "freshnessStatus": "pass",
                                    },
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
                                        "freshnessStatus": "warn",
                                    },
                                }
                            },
                        ],
                    }
                }
            }
        }
    }

    # Set up the mock to return our response
    mock_api_client.return_value = mock_response

    # Execute the fetch
    result = await sources_fetcher.fetch_sources(config=unit_discovery_config)

    # Verify the API was called correctly
    mock_api_client.assert_called_once()
    call_args = mock_api_client.call_args

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


@pytest.mark.parametrize(
    "filter_params,expected_filter",
    [
        # Single filter parameters
        ({"source_names": ["external_api"]}, {"sourceNames": ["external_api"]}),
        (
            {"unique_ids": ["source.test_project.raw_data.customers"]},
            {"uniqueIds": ["source.test_project.raw_data.customers"]},
        ),
        # Combined filters
        (
            {
                "source_names": ["core"],
                "unique_ids": ["source.test_project.core.users"],
            },
            {"sourceNames": ["core"], "uniqueIds": ["source.test_project.core.users"]},
        ),
    ],
)
async def test_fetch_sources_with_filters(
    sources_fetcher,
    mock_api_client,
    unit_discovery_config,
    filter_params,
    expected_filter,
):
    """Test that various filter parameters are correctly converted to GraphQL filter format."""
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
                                        "freshnessStatus": "pass",
                                    },
                                }
                            }
                        ],
                    }
                }
            }
        }
    }

    mock_api_client.return_value = mock_response

    # Execute with filters
    result = await sources_fetcher.fetch_sources(
        **filter_params, config=unit_discovery_config
    )

    # Verify the filter was passed correctly to the GraphQL query
    call_args = mock_api_client.call_args
    variables = call_args[0][1]
    assert variables["sourcesFilter"] == expected_filter

    # Verify the result structure
    assert isinstance(result, list)
    assert len(result) == 1


async def test_fetch_sources_empty_response(
    sources_fetcher, mock_api_client, unit_discovery_config
):
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "sources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "edges": [],
                    }
                }
            }
        }
    }

    mock_api_client.return_value = mock_response

    result = await sources_fetcher.fetch_sources(config=unit_discovery_config)

    assert result == []


async def test_fetch_sources_pagination(
    sources_fetcher, mock_api_client, unit_discovery_config
):
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
                                        "freshnessStatus": "pass",
                                    },
                                }
                            }
                        ],
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
                        "pageInfo": {
                            "hasNextPage": False,
                            "endCursor": "cursor_page_1",
                        },  # hasNextPage False stops pagination
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
                                        "freshnessStatus": "warn",
                                    },
                                }
                            }
                        ],
                    }
                }
            }
        }
    }

    # Set up mock to return different responses for each call
    mock_api_client.side_effect = [
        first_page_response,
        second_page_response,
    ]

    result = await sources_fetcher.fetch_sources(config=unit_discovery_config)

    # Should have called twice due to pagination
    assert mock_api_client.call_count == 2

    # Check that the second call includes the cursor from the first response
    first_call_args = mock_api_client.call_args_list[0]
    second_call_args = mock_api_client.call_args_list[1]

    # First call should have empty after cursor
    assert "after" not in first_call_args[0][1]

    # Second call should have the cursor from first response
    assert second_call_args[0][1]["after"] == "cursor_page_1"

    # Should have both results
    assert len(result) == 2
    assert result[0]["name"] == "customers"
    assert result[1]["name"] == "orders"


@patch("dbt_mcp.discovery.client.raise_gql_error")
async def test_fetch_sources_graphql_error_handling(
    mock_raise_gql_error, sources_fetcher, mock_api_client, unit_discovery_config
):
    mock_response = {
        "data": {
            "environment": {
                "applied": {
                    "sources": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "edges": [],
                    }
                }
            }
        }
    }

    # Configure the mock to raise GraphQLError when called
    mock_raise_gql_error.side_effect = GraphQLError("Test GraphQL error")

    mock_api_client.return_value = mock_response

    # Verify that fetch_sources raises GraphQLError
    with pytest.raises(GraphQLError, match="Test GraphQL error"):
        await sources_fetcher.fetch_sources(config=unit_discovery_config)

    # Verify that error handling function was called
    mock_raise_gql_error.assert_called_with(mock_response)
