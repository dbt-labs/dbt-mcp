from unittest.mock import patch

from dbt_mcp.config.config import load_config
from dbt_mcp.dbt_cli.binary_type import BinaryType
from dbt_mcp.mcp.server import create_dbt_mcp


async def test_disable_tools(env_setup):
    """Test that the ToolName enum matches the tools registered in the server."""
    disable_tools = {"get_mart_models", "list_metrics"}
    with (
        env_setup(
            env_vars={
                "DISABLE_TOOLS": ",".join(disable_tools),
            }
        ),
        patch(
            "dbt_mcp.config.config.detect_binary_type", return_value=BinaryType.DBT_CORE
        ),
        patch(
            "dbt_mcp.mcp.server.register_proxied_tools",
            return_value=None,
        ),
    ):
        config = load_config()
        dbt_mcp = await create_dbt_mcp(config)

        # Get all tools from the server
        server_tools = await dbt_mcp.list_tools()
        server_tool_names = {tool.name for tool in server_tools}
        assert not disable_tools.intersection(server_tool_names)


async def test_disable_cli_tools(env_setup):
    disable_tools = {"build", "compile", "docs", "list"}
    with (
        env_setup(
            env_vars={
                "DISABLE_TOOLS": ",".join(disable_tools),
            }
        ),
        patch(
            "dbt_mcp.config.config.detect_binary_type", return_value=BinaryType.DBT_CORE
        ),
    ):
        config = load_config(enable_proxied_tools=False)
        dbt_mcp = await create_dbt_mcp(config)

        # Get all tools from the server
        server_tools = await dbt_mcp.list_tools()
        server_tool_names = {tool.name for tool in server_tools}
        assert not disable_tools.intersection(server_tool_names)
        assert "show" in server_tool_names
