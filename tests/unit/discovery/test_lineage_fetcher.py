import pytest

from dbt_mcp.discovery.client import (
    LineageFetcher,
    LineageResourceType,
)
from dbt_mcp.errors import ToolCallError


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

    result = await lineage_fetcher.fetch_lineage(
        unique_id="model.test.customers", depth=5
    )

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
        depth=5,
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

    result = await lineage_fetcher.fetch_lineage(
        unique_id="model.test.nonexistent", depth=5
    )

    assert result == []


async def test_fetch_lineage_empty_response(lineage_fetcher, mock_api_client):
    """Test handling of empty API response."""
    mock_api_client.execute_query.return_value = {
        "data": {"environment": {"applied": {"lineage": []}}}
    }

    result = await lineage_fetcher.fetch_lineage(
        unique_id="model.test.customers", depth=5
    )

    assert result == []


async def test_fetch_lineage_filters_out_macros(lineage_fetcher, mock_api_client):
    """Test that macro nodes are filtered out from the lineage results."""
    mock_api_client.execute_query.return_value = {
        "data": {
            "environment": {
                "applied": {
                    "lineage": [
                        {
                            "uniqueId": "model.test.customers",
                            "name": "customers",
                            "resourceType": "Model",
                            "parentIds": ["macro.test.some_macro"],
                        },
                        {
                            "uniqueId": "macro.test.some_macro",
                            "name": "some_macro",
                            "resourceType": "Macro",
                            "parentIds": [],
                        },
                        {
                            "uniqueId": "macro.test.another_macro",
                            "name": "another_macro",
                            "resourceType": "macro",  # lowercase to test case-insensitivity
                            "parentIds": [],
                        },
                    ]
                }
            }
        }
    }

    result = await lineage_fetcher.fetch_lineage(
        unique_id="model.test.customers", depth=5
    )

    # Should only return the model, macros should be filtered out
    assert len(result) == 1
    assert result[0]["uniqueId"] == "model.test.customers"


async def test_fetch_lineage_depth_limits_traversal(lineage_fetcher, mock_api_client):
    """Test that depth parameter limits how far the BFS traverses."""
    # Create a chain: source -> model1 -> model2 -> model3 -> model4
    mock_api_client.execute_query.return_value = {
        "data": {
            "environment": {
                "applied": {
                    "lineage": [
                        {
                            "uniqueId": "source.test.raw",
                            "name": "raw",
                            "resourceType": "Source",
                            "parentIds": [],
                        },
                        {
                            "uniqueId": "model.test.model1",
                            "name": "model1",
                            "resourceType": "Model",
                            "parentIds": ["source.test.raw"],
                        },
                        {
                            "uniqueId": "model.test.model2",
                            "name": "model2",
                            "resourceType": "Model",
                            "parentIds": ["model.test.model1"],
                        },
                        {
                            "uniqueId": "model.test.model3",
                            "name": "model3",
                            "resourceType": "Model",
                            "parentIds": ["model.test.model2"],
                        },
                        {
                            "uniqueId": "model.test.model4",
                            "name": "model4",
                            "resourceType": "Model",
                            "parentIds": ["model.test.model3"],
                        },
                    ]
                }
            }
        }
    }

    # With depth=2, starting from model2, should include model1, model2, model3
    # (1 step upstream to model1, 1 step downstream to model3)
    result = await lineage_fetcher.fetch_lineage(unique_id="model.test.model2", depth=2)

    unique_ids = {node["uniqueId"] for node in result}
    # Depth 2 from model2: model2 (start), model1 (depth 1), source.raw (depth 2),
    # model3 (depth 1), model4 (depth 2)
    assert "model.test.model2" in unique_ids
    assert "model.test.model1" in unique_ids
    assert "model.test.model3" in unique_ids


async def test_fetch_lineage_depth_zero_raises_error(lineage_fetcher, mock_api_client):
    """Test that depth=0 raises a ToolCallError."""
    with pytest.raises(ToolCallError, match="Depth must be greater than 0"):
        await lineage_fetcher.fetch_lineage(unique_id="model.test.customers", depth=0)


async def test_fetch_lineage_depth_one_returns_immediate_neighbors(
    lineage_fetcher, mock_api_client
):
    """Test that depth=1 returns target and its immediate neighbors."""
    mock_api_client.execute_query.return_value = {
        "data": {
            "environment": {
                "applied": {
                    "lineage": [
                        {
                            "uniqueId": "source.test.raw",
                            "name": "raw",
                            "resourceType": "Source",
                            "parentIds": [],
                        },
                        {
                            "uniqueId": "model.test.staging",
                            "name": "staging",
                            "resourceType": "Model",
                            "parentIds": ["source.test.raw"],
                        },
                        {
                            "uniqueId": "model.test.mart",
                            "name": "mart",
                            "resourceType": "Model",
                            "parentIds": ["model.test.staging"],
                        },
                    ]
                }
            }
        }
    }

    result = await lineage_fetcher.fetch_lineage(
        unique_id="model.test.staging", depth=1
    )

    unique_ids = {node["uniqueId"] for node in result}
    # Depth 1 from staging: staging (start), raw (parent, depth 1), mart (child, depth 1)
    assert unique_ids == {
        "model.test.staging",
        "source.test.raw",
        "model.test.mart",
    }


async def test_fetch_lineage_filters_nodes_without_resource_type(
    lineage_fetcher, mock_api_client
):
    """Test that nodes without a resourceType are filtered out."""
    mock_api_client.execute_query.return_value = {
        "data": {
            "environment": {
                "applied": {
                    "lineage": [
                        {
                            "uniqueId": "model.test.customers",
                            "name": "customers",
                            "resourceType": "Model",
                            "parentIds": ["node.test.missing_type"],
                        },
                        {
                            "uniqueId": "node.test.missing_type",
                            "name": "missing_type",
                            # No resourceType field
                            "parentIds": [],
                        },
                        {
                            "uniqueId": "node.test.null_type",
                            "name": "null_type",
                            "resourceType": None,
                            "parentIds": [],
                        },
                    ]
                }
            }
        }
    }

    result = await lineage_fetcher.fetch_lineage(
        unique_id="model.test.customers", depth=5
    )

    # Should only return the model with valid resourceType
    assert len(result) == 1
    assert result[0]["uniqueId"] == "model.test.customers"


async def test_fetch_lineage_depth_excludes_nodes_beyond_limit(
    lineage_fetcher, mock_api_client
):
    """Test that nodes beyond the depth limit are explicitly excluded.

    This test guards against a bug where the BFS loop terminates prematurely
    due to checking depth before processing queued items.

    Chain: source -> stg -> int -> mart -> agg
    Starting from 'int' with depth=1:
    - int (depth 0, start) - INCLUDED
    - stg (depth 1, parent) - INCLUDED
    - mart (depth 1, child) - INCLUDED
    - source (depth 2, beyond limit) - EXCLUDED
    - agg (depth 2, beyond limit) - EXCLUDED
    """
    mock_api_client.execute_query.return_value = {
        "data": {
            "environment": {
                "applied": {
                    "lineage": [
                        {
                            "uniqueId": "source.test.raw",
                            "name": "raw",
                            "resourceType": "Source",
                            "parentIds": [],
                        },
                        {
                            "uniqueId": "model.test.stg",
                            "name": "stg",
                            "resourceType": "Model",
                            "parentIds": ["source.test.raw"],
                        },
                        {
                            "uniqueId": "model.test.int",
                            "name": "int",
                            "resourceType": "Model",
                            "parentIds": ["model.test.stg"],
                        },
                        {
                            "uniqueId": "model.test.mart",
                            "name": "mart",
                            "resourceType": "Model",
                            "parentIds": ["model.test.int"],
                        },
                        {
                            "uniqueId": "model.test.agg",
                            "name": "agg",
                            "resourceType": "Model",
                            "parentIds": ["model.test.mart"],
                        },
                    ]
                }
            }
        }
    }

    result = await lineage_fetcher.fetch_lineage(unique_id="model.test.int", depth=1)

    unique_ids = {node["uniqueId"] for node in result}

    # Assert exactly which nodes are included
    assert unique_ids == {
        "model.test.int",  # depth 0 (start)
        "model.test.stg",  # depth 1 (parent)
        "model.test.mart",  # depth 1 (child)
    }

    # Explicitly assert which nodes are excluded
    assert "source.test.raw" not in unique_ids, "source should be excluded (depth 2)"
    assert "model.test.agg" not in unique_ids, "agg should be excluded (depth 2)"


async def test_fetch_lineage_depth_processes_all_queued_items_at_valid_depths(
    lineage_fetcher, mock_api_client
):
    """Test that BFS processes all items in queue at valid depths.

    This specifically guards against the bug where:
        while queue and current_depth < depth:
            current_id, current_depth = queue.pop(0)

    Would terminate the loop when an item at depth=N is popped but the condition
    checks current_depth from a previous iteration.

    Diamond pattern:
           source
          /      \\
        left    right
          \\      /
           target

    Starting from 'target' with depth=1, both 'left' and 'right' should be included.
    """
    mock_api_client.execute_query.return_value = {
        "data": {
            "environment": {
                "applied": {
                    "lineage": [
                        {
                            "uniqueId": "source.test.raw",
                            "name": "raw",
                            "resourceType": "Source",
                            "parentIds": [],
                        },
                        {
                            "uniqueId": "model.test.left",
                            "name": "left",
                            "resourceType": "Model",
                            "parentIds": ["source.test.raw"],
                        },
                        {
                            "uniqueId": "model.test.right",
                            "name": "right",
                            "resourceType": "Model",
                            "parentIds": ["source.test.raw"],
                        },
                        {
                            "uniqueId": "model.test.target",
                            "name": "target",
                            "resourceType": "Model",
                            "parentIds": ["model.test.left", "model.test.right"],
                        },
                    ]
                }
            }
        }
    }

    result = await lineage_fetcher.fetch_lineage(unique_id="model.test.target", depth=1)

    unique_ids = {node["uniqueId"] for node in result}

    # Both parents at depth 1 must be included
    assert unique_ids == {
        "model.test.target",  # depth 0
        "model.test.left",  # depth 1
        "model.test.right",  # depth 1
    }

    # Source is at depth 2, should be excluded
    assert "source.test.raw" not in unique_ids


async def test_fetch_lineage_depth_boundary_includes_nodes_at_exact_depth(
    lineage_fetcher, mock_api_client
):
    """Test that nodes at exactly the depth limit ARE included in results.

    Chain: A -> B -> C -> D -> E
    Starting from C with depth=2:
    - C (depth 0) - INCLUDED
    - B (depth 1) - INCLUDED
    - D (depth 1) - INCLUDED
    - A (depth 2) - INCLUDED (at boundary)
    - E (depth 2) - INCLUDED (at boundary)

    All 5 nodes should be included since depth=2 allows traversal to depth 2.
    """
    mock_api_client.execute_query.return_value = {
        "data": {
            "environment": {
                "applied": {
                    "lineage": [
                        {
                            "uniqueId": "model.test.a",
                            "name": "a",
                            "resourceType": "Model",
                            "parentIds": [],
                        },
                        {
                            "uniqueId": "model.test.b",
                            "name": "b",
                            "resourceType": "Model",
                            "parentIds": ["model.test.a"],
                        },
                        {
                            "uniqueId": "model.test.c",
                            "name": "c",
                            "resourceType": "Model",
                            "parentIds": ["model.test.b"],
                        },
                        {
                            "uniqueId": "model.test.d",
                            "name": "d",
                            "resourceType": "Model",
                            "parentIds": ["model.test.c"],
                        },
                        {
                            "uniqueId": "model.test.e",
                            "name": "e",
                            "resourceType": "Model",
                            "parentIds": ["model.test.d"],
                        },
                    ]
                }
            }
        }
    }

    result = await lineage_fetcher.fetch_lineage(unique_id="model.test.c", depth=2)

    unique_ids = {node["uniqueId"] for node in result}

    # All nodes within depth 2 should be included
    assert unique_ids == {
        "model.test.a",  # depth 2 (boundary)
        "model.test.b",  # depth 1
        "model.test.c",  # depth 0 (start)
        "model.test.d",  # depth 1
        "model.test.e",  # depth 2 (boundary)
    }


async def test_fetch_lineage_large_depth_returns_all_connected(
    lineage_fetcher, mock_api_client
):
    """Test that a large depth value returns all connected nodes."""
    mock_api_client.execute_query.return_value = {
        "data": {
            "environment": {
                "applied": {
                    "lineage": [
                        {
                            "uniqueId": "model.test.a",
                            "name": "a",
                            "resourceType": "Model",
                            "parentIds": [],
                        },
                        {
                            "uniqueId": "model.test.b",
                            "name": "b",
                            "resourceType": "Model",
                            "parentIds": ["model.test.a"],
                        },
                        {
                            "uniqueId": "model.test.c",
                            "name": "c",
                            "resourceType": "Model",
                            "parentIds": ["model.test.b"],
                        },
                    ]
                }
            }
        }
    }

    result = await lineage_fetcher.fetch_lineage(unique_id="model.test.b", depth=100)

    unique_ids = {node["uniqueId"] for node in result}

    # All connected nodes should be included with a large depth
    assert unique_ids == {
        "model.test.a",
        "model.test.b",
        "model.test.c",
    }
