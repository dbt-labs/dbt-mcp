from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import load_config
from dbt_mcp.remote.tools import register_remote_tools


async def test_remote_tool_execute_sql():
    config = load_config()
    dbt_mcp = FastMCP("Test")
    await register_remote_tools(dbt_mcp, config.remote_config)
    tools = await dbt_mcp.list_tools()
    print(tools)
    result = await dbt_mcp.call_tool("execute_sql", {"sql": "SELECT 1"})
    assert len(result) == 1
    assert "1" in result[0].text


async def test_remote_tool_text_to_sql():
    config = load_config()
    dbt_mcp = FastMCP("Test")
    await register_remote_tools(dbt_mcp, config.remote_config)
    result = await dbt_mcp.call_tool("text_to_sql", {"text": "SELECT 1"})
    assert len(result) == 1
    assert "SELECT 1" in result[0].text
