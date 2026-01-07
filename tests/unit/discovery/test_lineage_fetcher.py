import pytest

from dbt_mcp.discovery.client import (
    LineageFetcher,
    LineageResourceType,
)


@pytest.fixture
def lineage_fetcher(mock_api_client):
    return LineageFetcher(api_client=mock_api_client)


async def test_fetch_lineage_returns_connected_nodes(lineage_fetcher, mock_api_client):
    """Test that fetch_lineage returns only nodes connected to the target."""
    mock_api_client.execute_query.return_value = {
        "data": {
            "environment": {
                "applied": {
                    "lineage": [
                        # Connected subgraph
                        {
                            "uniqueId": "model.test.customers",
                            "name": "customers",
                            "resourceType": "Model",
                            "parentIds": ["source.test.raw_customers"],
                        },
                        {
                            "uniqueId": "source.test.raw_customers",
                            "name": "raw_customers",
                            "resourceType": "Source",
                            "parentIds": [],
                        },
                        {
                            "uniqueId": "model.test.customer_metrics",
                            "name": "customer_metrics",
                            "resourceType": "Model",
                            "parentIds": ["model.test.customers"],
                        },
                        # Disconnected subgraph (should be filtered out)
                        {
                            "uniqueId": "model.test.orders",
                            "name": "orders",
                            "resourceType": "Model",
                            "parentIds": ["source.test.raw_orders"],
                        },
                        {
                            "uniqueId": "source.test.raw_orders",
                            "name": "raw_orders",
                            "resourceType": "Source",
                            "parentIds": [],
                        },
                    ]
                }
            }
        }
    }

    result = await lineage_fetcher.fetch_lineage(unique_id="model.test.customers")

    # Should return only the 3 connected nodes
    assert len(result) == 3
    unique_ids = {node["uniqueId"] for node in result}
    assert unique_ids == {
        "model.test.customers",
        "source.test.raw_customers",
        "model.test.customer_metrics",
    }


async def test_fetch_lineage_with_type_filter(lineage_fetcher, mock_api_client):
    """Test that type filter is passed to the API."""
    mock_api_client.execute_query.return_value = {
        "data": {"environment": {"applied": {"lineage": []}}}
    }

    await lineage_fetcher.fetch_lineage(
        unique_id="model.test.customers",
        types=[LineageResourceType.MODEL, LineageResourceType.SOURCE],
    )

    call_args = mock_api_client.execute_query.call_args
    variables = call_args[0][1]
    assert set(variables["types"]) == {"Model", "Source"}


async def test_fetch_lineage_target_not_found(lineage_fetcher, mock_api_client):
    """Test that empty list is returned when target is not in the graph."""
    mock_api_client.execute_query.return_value = {
        "data": {
            "environment": {
                "applied": {
                    "lineage": [
                        {
                            "uniqueId": "model.test.other",
                            "name": "other",
                            "resourceType": "Model",
                            "parentIds": [],
                        }
                    ]
                }
            }
        }
    }

    result = await lineage_fetcher.fetch_lineage(unique_id="model.test.nonexistent")

    assert result == []


async def test_fetch_lineage_empty_response(lineage_fetcher, mock_api_client):
    """Test handling of empty API response."""
    mock_api_client.execute_query.return_value = {
        "data": {"environment": {"applied": {"lineage": []}}}
    }

    result = await lineage_fetcher.fetch_lineage(unique_id="model.test.customers")

    assert result == []
