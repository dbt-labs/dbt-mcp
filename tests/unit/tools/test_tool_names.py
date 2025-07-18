import pytest

from dbt_mcp.mcp.server import create_dbt_mcp
from dbt_mcp.tools.tool_names import ToolName


@pytest.mark.asyncio
async def test_tool_names_match_server_tools():
    """Test that the ToolName enum matches the tools registered in the server."""
    # Create the dbt_mcp server instance
    dbt_mcp = await create_dbt_mcp()

    # Get all tools from the server
    server_tools = await dbt_mcp.list_tools()
    server_tool_names = {tool.name for tool in server_tools}
    enum_names = ToolName.get_all_tool_names()

    # This should not raise any errors if the enum is in sync
    if server_tool_names != enum_names:
        raise ValueError(
            f"Tool name mismatch:\n"
            f"In server but not in enum: {server_tool_names - enum_names}\n"
            f"In enum but not in server: {enum_names - server_tool_names}"
        )

    # Double check that all enum values are strings
    for tool in ToolName:
        assert isinstance(tool.value, str), f"Tool {tool.name} value should be a string"
