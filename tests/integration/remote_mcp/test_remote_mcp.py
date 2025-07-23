from dbt_mcp.config.config import load_config
from dbt_mcp.mcp.server import create_dbt_mcp
from remote_mcp.session import session_context


async def test_remote_mcp_list_metrics_equals_local_mcp() -> None:
    async with session_context() as session:
        config = load_config()
        dbt_mcp = await create_dbt_mcp(config)

        remote_metrics = await session.call_tool(
            name="list_metrics",
            arguments={},
        )
        local_metrics = await dbt_mcp.call_tool(
            name="list_metrics",
            arguments={},
        )
        assert remote_metrics.content == local_metrics
