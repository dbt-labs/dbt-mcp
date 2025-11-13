from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import load_config
from dbt_mcp.proxy.tools import register_proxied_tools


async def test_proxied_tool_execute_sql():
    config = load_config()
    dbt_mcp = FastMCP("Test")
    assert config.proxied_tool_config_provider is not None
    await register_proxied_tools(dbt_mcp, config.proxied_tool_config_provider)
    result = await dbt_mcp.call_tool("execute_sql", {"sql": "SELECT 1"})
    assert len(result) == 1
    assert "1" in result[0].text


async def test_proxied_tool_text_to_sql():
    config = load_config()
    dbt_mcp = FastMCP("Test")
    assert config.proxied_tool_config_provider is not None
    await register_proxied_tools(dbt_mcp, config.proxied_tool_config_provider)
    result = await dbt_mcp.call_tool("text_to_sql", {"text": "SELECT 1"})
    assert len(result) == 1
    assert "SELECT 1" in result[0].text
