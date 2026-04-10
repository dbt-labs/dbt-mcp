import json

from dbt_mcp.config.config import load_config
from dbt_mcp.mcp.server import create_dbt_mcp
from remote_mcp.session import session_context


# Note: local and remote list_metrics responses are intentionally not compared here.
# The remote MCP runs the deployed version of the server, which may lag behind local
# changes. After list_metrics was updated to return a ListMetricsResponse
# (metrics + optional dimensions/entities), the two responses will differ until the
# remote deploys the same version.
async def test_local_mcp_list_metrics_returns_valid_response() -> None:
    config = load_config()
    dbt_mcp = await create_dbt_mcp(config)
    result = await dbt_mcp.call_tool(
        name="list_metrics",
        arguments={},
    )
    assert isinstance(result, list)
    assert len(result) == 1
    content = result[0]
    assert hasattr(content, "text")
    payload = json.loads(content.text)  # type: ignore[union-attr]
    assert "metrics" in payload
    assert len(payload["metrics"]) > 0


async def test_remote_mcp_list_metrics_returns_metrics() -> None:
    async with session_context() as session:
        remote_metrics = await session.call_tool(
            name="list_metrics",
            arguments={},
        )
    assert not remote_metrics.isError
    assert len(remote_metrics.content) > 0
