from dbt_mcp.config.config import load_config
from dbt_mcp.mcp.server import create_dbt_mcp
from dbt_mcp.tools.toolsets import toolsets
from tests.env_vars import default_env_vars_context


async def test_toolsets_match_server_tools():
    """Test that the defined toolsets match the tools registered in the server."""
    sql_tool_names = {"text_to_sql", "execute_sql"}

    with default_env_vars_context():
        config = load_config()
        dbt_mcp = await create_dbt_mcp(config)

        # Get all tools from the server
        server_tools = await dbt_mcp.list_tools()
        # Manually adding SQL tools here because the server doesn't get them
        # in this unit test.
        server_tool_names = {tool.name for tool in server_tools} | sql_tool_names
        defined_tools = set()
        for toolset_tools in toolsets.values():
            defined_tools.update({t.value for t in toolset_tools})

        if server_tool_names != defined_tools:
            raise ValueError(
                f"Tool name mismatch:\n"
                f"In server but not in enum: {server_tool_names - defined_tools}\n"
                f"In enum but not in server: {defined_tools - server_tool_names}"
            )
