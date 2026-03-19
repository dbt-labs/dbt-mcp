import json
from unittest.mock import AsyncMock, Mock

import pytest
from mcp.types import CallToolResult, TextContent

from dbt_mcp.discovery.tools import (
    DiscoveryToolContext,
    get_lineage as get_lineage_tool,
)

# Access the underlying function from the ToolDefinition
get_lineage = get_lineage_tool.fn


@pytest.fixture
def mock_discovery_tool_context():
    """Mock DiscoveryToolContext for testing."""
    context = Mock(spec=DiscoveryToolContext)
    context.lineage_fetcher = AsyncMock()
    return context


SAMPLE_NODES = [
    {
        "uniqueId": "source.test.raw_customers",
        "name": "raw_customers",
        "resourceType": "Source",
        "parentIds": [],
    },
    {
        "uniqueId": "model.test.customers",
        "name": "customers",
        "resourceType": "Model",
        "parentIds": ["source.test.raw_customers"],
    },
    {
        "uniqueId": "model.test.customer_metrics",
        "name": "customer_metrics",
        "resourceType": "Model",
        "parentIds": ["model.test.customers"],
    },
]


async def test_get_lineage_returns_call_tool_result(mock_discovery_tool_context):
    """Test that get_lineage returns a CallToolResult."""
    mock_discovery_tool_context.lineage_fetcher.fetch_lineage.return_value = (
        SAMPLE_NODES
    )

    result = await get_lineage(
        context=mock_discovery_tool_context,
        unique_id="model.test.customers",
        types=None,
        depth=5,
    )

    assert isinstance(result, CallToolResult)


async def test_get_lineage_text_content_contains_raw_nodes(mock_discovery_tool_context):
    """Test that the text content contains the raw node data as JSON."""
    mock_discovery_tool_context.lineage_fetcher.fetch_lineage.return_value = (
        SAMPLE_NODES
    )

    result = await get_lineage(
        context=mock_discovery_tool_context,
        unique_id="model.test.customers",
        types=None,
        depth=5,
    )

    assert len(result.content) == 1
    assert isinstance(result.content[0], TextContent)
    parsed = json.loads(result.content[0].text)
    assert parsed == SAMPLE_NODES


async def test_get_lineage_structured_content_has_correct_graph(
    mock_discovery_tool_context,
):
    """Test that structuredContent contains a well-formed LineageGraph."""
    mock_discovery_tool_context.lineage_fetcher.fetch_lineage.return_value = (
        SAMPLE_NODES
    )

    result = await get_lineage(
        context=mock_discovery_tool_context,
        unique_id="model.test.customers",
        types=None,
        depth=5,
    )

    sc = result.structuredContent
    assert sc["type"] == "lineage_graph"
    assert sc["root_id"] == "model.test.customers"
    assert len(sc["nodes"]) == 3
    assert len(sc["edges"]) == 2

    node_ids = {n["unique_id"] for n in sc["nodes"]}
    assert node_ids == {
        "source.test.raw_customers",
        "model.test.customers",
        "model.test.customer_metrics",
    }

    edges = {(e["source"], e["target"]) for e in sc["edges"]}
    assert edges == {
        ("source.test.raw_customers", "model.test.customers"),
        ("model.test.customers", "model.test.customer_metrics"),
    }


async def test_get_lineage_filters_edges_to_known_nodes(mock_discovery_tool_context):
    """Test that edges referencing nodes outside the graph are excluded."""
    nodes_with_external_parent = [
        {
            "uniqueId": "model.test.customers",
            "name": "customers",
            "resourceType": "Model",
            "parentIds": ["source.test.raw_customers", "model.other.unknown"],
        },
        {
            "uniqueId": "source.test.raw_customers",
            "name": "raw_customers",
            "resourceType": "Source",
            "parentIds": [],
        },
    ]
    mock_discovery_tool_context.lineage_fetcher.fetch_lineage.return_value = (
        nodes_with_external_parent
    )

    result = await get_lineage(
        context=mock_discovery_tool_context,
        unique_id="model.test.customers",
        types=None,
        depth=5,
    )

    sc = result.structuredContent
    # Only the edge from raw_customers should exist; model.other.unknown is not in the graph
    assert len(sc["edges"]) == 1
    assert sc["edges"][0]["source"] == "source.test.raw_customers"
    assert sc["edges"][0]["target"] == "model.test.customers"


async def test_get_lineage_empty_result(mock_discovery_tool_context):
    """Test that an empty fetcher result produces an empty graph."""
    mock_discovery_tool_context.lineage_fetcher.fetch_lineage.return_value = []

    result = await get_lineage(
        context=mock_discovery_tool_context,
        unique_id="model.test.nonexistent",
        types=None,
        depth=5,
    )

    sc = result.structuredContent
    assert sc["nodes"] == []
    assert sc["edges"] == []
    assert sc["root_id"] == "model.test.nonexistent"


async def test_get_lineage_passes_parameters_to_fetcher(mock_discovery_tool_context):
    """Test that parameters are correctly forwarded to the fetcher."""
    mock_discovery_tool_context.lineage_fetcher.fetch_lineage.return_value = []

    await get_lineage(
        context=mock_discovery_tool_context,
        unique_id="model.test.customers",
        types=["Model", "Source"],
        depth=3,
    )

    mock_discovery_tool_context.lineage_fetcher.fetch_lineage.assert_called_once_with(
        unique_id="model.test.customers",
        types=["Model", "Source"],
        depth=3,
    )


async def test_get_lineage_node_without_parent_ids(mock_discovery_tool_context):
    """Test handling of nodes that lack a parentIds field."""
    nodes = [
        {
            "uniqueId": "model.test.orphan",
            "name": "orphan",
            "resourceType": "Model",
            # no parentIds key
        },
    ]
    mock_discovery_tool_context.lineage_fetcher.fetch_lineage.return_value = nodes

    result = await get_lineage(
        context=mock_discovery_tool_context,
        unique_id="model.test.orphan",
        types=None,
        depth=5,
    )

    sc = result.structuredContent
    assert len(sc["nodes"]) == 1
    assert sc["edges"] == []
