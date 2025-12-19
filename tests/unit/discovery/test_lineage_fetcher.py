import pytest

from dbt_mcp.discovery.client import (
    LineageFetcher,
    LineageResourceType,
)


@pytest.fixture
def lineage_fetcher(mock_api_client):
    return LineageFetcher(api_client=mock_api_client)


@pytest.fixture
def mock_lineage_response():
    """Sample lineage response from GraphQL API."""
    return {
        "data": {
            "environment": {
                "applied": {
                    "lineage": [
                        {
                            "name": "customers",
                            "uniqueId": "model.test_project.customers",
                            "resourceType": "Model",
                            "parentIds": [
                                "model.test_project.stg_customers",
                                "source.test_project.raw.customers",
                            ],
                        },
                        {
                            "name": "stg_customers",
                            "uniqueId": "model.test_project.stg_customers",
                            "resourceType": "Model",
                            "parentIds": ["source.test_project.raw.customers"],
                        },
                        {
                            "name": "customers",
                            "uniqueId": "source.test_project.raw.customers",
                            "resourceType": "Source",
                            "parentIds": [],
                        },
                    ]
                }
            }
        }
    }


async def test_fetch_lineage_returns_connected_nodes(
    lineage_fetcher, mock_api_client, mock_lineage_response
):
    """Test fetching lineage returns connected nodes from the API."""
    mock_api_client.execute_query.return_value = mock_lineage_response

    result = await lineage_fetcher.fetch_lineage(
        unique_id="model.test_project.customers"
    )

    # Verify API was called
    mock_api_client.execute_query.assert_called_once()

    # Verify nodes returned
    assert len(result) == 3
    unique_ids = {node["uniqueId"] for node in result}
    assert "model.test_project.customers" in unique_ids
    assert "model.test_project.stg_customers" in unique_ids
    assert "source.test_project.raw.customers" in unique_ids


async def test_fetch_lineage_filters_to_connected_subgraph(
    lineage_fetcher, mock_api_client
):
    """Test that lineage returns only nodes connected to the target."""
    # Mock response with disconnected nodes
    mock_api_client.execute_query.return_value = {
        "data": {
            "environment": {
                "applied": {
                    "lineage": [
                        # Connected to customers
                        {
                            "name": "customers",
                            "uniqueId": "model.test.customers",
                            "resourceType": "Model",
                            "parentIds": ["source.test.raw_customers"],
                        },
                        {
                            "name": "raw_customers",
                            "uniqueId": "source.test.raw_customers",
                            "resourceType": "Source",
                            "parentIds": [],
                        },
                        {
                            "name": "customer_metrics",
                            "uniqueId": "model.test.customer_metrics",
                            "resourceType": "Model",
                            "parentIds": ["model.test.customers"],
                        },
                        # NOT connected to customers
                        {
                            "name": "orders",
                            "uniqueId": "model.test.orders",
                            "resourceType": "Model",
                            "parentIds": ["source.test.raw_orders"],
                        },
                        {
                            "name": "raw_orders",
                            "uniqueId": "source.test.raw_orders",
                            "resourceType": "Source",
                            "parentIds": [],
                        },
                    ]
                }
            }
        }
    }

    result = await lineage_fetcher.fetch_lineage(unique_id="model.test.customers")

    # Should only include the 3 nodes connected to customers
    assert len(result) == 3
    unique_ids = {node["uniqueId"] for node in result}
    assert "model.test.customers" in unique_ids
    assert "source.test.raw_customers" in unique_ids
    assert "model.test.customer_metrics" in unique_ids
    # Should NOT include disconnected nodes
    assert "model.test.orders" not in unique_ids
    assert "source.test.raw_orders" not in unique_ids


async def test_fetch_lineage_with_type_filter(
    lineage_fetcher, mock_api_client, mock_lineage_response
):
    """Test fetching lineage with type filter."""
    mock_api_client.execute_query.return_value = mock_lineage_response

    await lineage_fetcher.fetch_lineage(
        unique_id="model.test_project.customers",
        types=[LineageResourceType.MODEL, LineageResourceType.SOURCE],
    )

    # Verify API was called with correct types
    mock_api_client.execute_query.assert_called_once()
    call_args = mock_api_client.execute_query.call_args
    variables = call_args[0][1]
    assert set(variables["types"]) == {"Model", "Source"}


async def test_fetch_lineage_empty_response(lineage_fetcher, mock_api_client):
    """Test handling of empty lineage response."""
    mock_api_client.execute_query.return_value = {
        "data": {"environment": {"applied": {"lineage": []}}}
    }

    result = await lineage_fetcher.fetch_lineage(
        unique_id="model.test_project.nonexistent"
    )

    assert result == []


async def test_fetch_lineage_target_not_found(
    lineage_fetcher, mock_api_client, mock_lineage_response
):
    """Test that empty list is returned when target not in graph."""
    mock_api_client.execute_query.return_value = mock_lineage_response

    result = await lineage_fetcher.fetch_lineage(
        unique_id="model.test_project.does_not_exist"
    )

    assert result == []


async def test_fetch_lineage_preserves_parent_ids(
    lineage_fetcher, mock_api_client, mock_lineage_response
):
    """Test that parentIds are preserved in the response."""
    mock_api_client.execute_query.return_value = mock_lineage_response

    result = await lineage_fetcher.fetch_lineage(
        unique_id="model.test_project.customers"
    )

    # Find the customers model and check its parentIds
    customers = next(
        n for n in result if n["uniqueId"] == "model.test_project.customers"
    )
    assert "model.test_project.stg_customers" in customers["parentIds"]
    assert "source.test_project.raw.customers" in customers["parentIds"]


async def test_fetch_lineage_filters_disconnected_nodes(
    lineage_fetcher, mock_api_client
):
    """Test that only connected nodes are returned, not the entire graph."""
    # Mock API returns 5 nodes, but only 3 are connected to customers
    mock_api_client.execute_query.return_value = {
        "data": {
            "environment": {
                "applied": {
                    "lineage": [
                        # Connected to customers
                        {
                            "name": "customers",
                            "uniqueId": "model.test.customers",
                            "resourceType": "Model",
                            "parentIds": ["source.test.raw_customers"],
                        },
                        {
                            "name": "raw_customers",
                            "uniqueId": "source.test.raw_customers",
                            "resourceType": "Source",
                            "parentIds": [],
                        },
                        {
                            "name": "customer_metrics",
                            "uniqueId": "model.test.customer_metrics",
                            "resourceType": "Model",
                            "parentIds": ["model.test.customers"],
                        },
                        # NOT connected to customers
                        {
                            "name": "orders",
                            "uniqueId": "model.test.orders",
                            "resourceType": "Model",
                            "parentIds": ["source.test.raw_orders"],
                        },
                        {
                            "name": "raw_orders",
                            "uniqueId": "source.test.raw_orders",
                            "resourceType": "Source",
                            "parentIds": [],
                        },
                    ]
                }
            }
        }
    }

    result = await lineage_fetcher.fetch_lineage(unique_id="model.test.customers")

    # Should only return the 3 connected nodes
    assert len(result) == 3
    unique_ids = {node["uniqueId"] for node in result}
    assert "model.test.customers" in unique_ids
    assert "source.test.raw_customers" in unique_ids
    assert "model.test.customer_metrics" in unique_ids
    # Should NOT include disconnected nodes
    assert "model.test.orders" not in unique_ids
    assert "source.test.raw_orders" not in unique_ids
