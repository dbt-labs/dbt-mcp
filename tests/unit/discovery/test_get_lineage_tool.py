from unittest.mock import AsyncMock, MagicMock

from dbt_mcp.discovery.tools import (
    LineageEdge,
    LineageGraph,
    get_lineage,
)


async def test_get_lineage_builds_graph_from_nodes():
    """get_lineage maps the fetcher's nodes into a LineageGraph (nodes + edges),
    deriving edges from parentIds and dropping parents outside the node set."""
    nodes = [
        {"uniqueId": "source.p.s", "name": "s", "resourceType": "Source"},
        {
            "uniqueId": "model.p.a",
            "name": "a",
            "resourceType": "Model",
            "parentIds": ["source.p.s"],
        },
        {
            "uniqueId": "model.p.b",
            "name": "b",
            "resourceType": "Model",
            # model.p.missing is not in the node set -> that edge is dropped
            "parentIds": ["model.p.a", "model.p.missing"],
        },
    ]
    context = MagicMock()
    context.config_provider.get_config = AsyncMock(return_value=MagicMock())
    context.lineage_fetcher.fetch_lineage = AsyncMock(return_value=nodes)

    result = await get_lineage.fn(
        context=context, unique_id="model.p.a", types=None, depth=2
    )

    assert isinstance(result, LineageGraph)
    assert result.type == "lineage_graph"
    assert result.root_id == "model.p.a"
    assert {n.unique_id for n in result.nodes} == {
        "source.p.s",
        "model.p.a",
        "model.p.b",
    }
    assert LineageEdge(source="source.p.s", target="model.p.a") in result.edges
    assert LineageEdge(source="model.p.a", target="model.p.b") in result.edges
    # parent outside the node set is not emitted as an edge
    assert all(edge.source != "model.p.missing" for edge in result.edges)
    assert len(result.edges) == 2
