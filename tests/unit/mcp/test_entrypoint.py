import asyncio
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def test_direct_script_entrypoint_initializes_and_lists_tools(tmp_path):
    script = Path(__file__).parents[3] / "src" / "dbt_mcp" / "main.py"
    parameters = StdioServerParameters(
        command=sys.executable,
        args=[str(script)],
        cwd=str(tmp_path),
        env={
            "MCP_TRANSPORT": "stdio",
            "DBT_HOST": "cloud.example",
            "DBT_TOKEN": "test-token",
            "DBT_ACCOUNT_ID": "1",
            "DBT_PROD_ENV_ID": "1",
            "DBT_PROJECT_DIR": "",
            "DISABLE_DBT_CLI": "true",
            "DISABLE_LSP": "true",
            "DISABLE_MCP_APPS": "true",
            "DBT_MCP_ENABLE_TOOLS": "list_jobs",
            "DO_NOT_TRACK": "1",
        },
    )
    async with asyncio.timeout(15):
        async with stdio_client(parameters) as streams:
            async with ClientSession(*streams) as session:
                initialized = await session.initialize()
                tools = await session.list_tools()

    assert initialized.serverInfo.name == "dbt"
    assert [tool.name for tool in tools.tools] == ["list_jobs"]
