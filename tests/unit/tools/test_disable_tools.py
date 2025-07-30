from dbt_mcp.config.config import load_config
from dbt_mcp.mcp.server import create_dbt_mcp
from tests.env_vars import default_env_vars_context


async def test_disable_tools():
    """Test that the ToolName enum matches the tools registered in the server."""
    disable_tools = {"get_mart_models", "list_metrics"}
    with default_env_vars_context(
        override_env_vars={"DISABLE_TOOLS": ",".join(disable_tools)}
    ):
        config = load_config()
        dbt_mcp = await create_dbt_mcp(config)

        # Get all tools from the server
        server_tools = await dbt_mcp.list_tools()
        server_tool_names = {tool.name for tool in server_tools}
        assert not disable_tools.intersection(server_tool_names)


async def test_disable_cli_tools():
    disable_tools = {"build", "compile", "docs", "list"}
    with default_env_vars_context(
        override_env_vars={"DISABLE_TOOLS": ",".join(disable_tools)}
    ):
        config = load_config()
        dbt_mcp = await create_dbt_mcp(config)

        # Get all tools from the server
        server_tools = await dbt_mcp.list_tools()
        server_tool_names = {tool.name for tool in server_tools}
        assert not disable_tools.intersection(server_tool_names)
        assert "show" in server_tool_names
