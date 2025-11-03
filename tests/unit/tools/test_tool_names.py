from unittest.mock import patch

import pytest

from dbt_mcp.config.config import load_config
from dbt_mcp.dbt_cli.binary_type import BinaryType
from dbt_mcp.lsp.lsp_binary_manager import LspBinaryInfo
from dbt_mcp.mcp.server import create_dbt_mcp
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import proxied_tools
from tests.env_vars import default_env_vars_context


@pytest.mark.asyncio
async def test_tool_names_match_server_tools():
    """Test that the ToolName enum matches the tools registered in the server."""
    with (
        default_env_vars_context(),
        patch(
            "dbt_mcp.config.config.detect_binary_type", return_value=BinaryType.DBT_CORE
        ),
        patch(
            "dbt_mcp.config.config.dbt_lsp_binary_info",
            return_value=LspBinaryInfo(path="/path/to/lsp", version="1.0.0"),
        ),
    ):
        config = load_config()
        dbt_mcp = await create_dbt_mcp(config)

        # Get all tools from the server
        server_tools = await dbt_mcp.list_tools()
        # Manually adding SQL tools here because the server doesn't get them
        # in this unit test.
        server_tool_names = {tool.name for tool in server_tools} | {
            p.value for p in proxied_tools
        }
        enum_names = {n for n in ToolName.get_all_tool_names()}

        # This should not raise any errors if the enum is in sync
        if server_tool_names != enum_names:
            raise ValueError(
                f"Tool name mismatch:\n"
                f"In server but not in enum: {server_tool_names - enum_names}\n"
                f"In enum but not in server: {enum_names - server_tool_names}"
            )

        # Double check that all enum values are strings
        for tool in ToolName:
            assert isinstance(tool.value, str), (
                f"Tool {tool.name} value should be a string"
            )


def test_tool_names_no_duplicates():
    """Test that there are no duplicate tool names in the enum."""
    assert len(ToolName.get_all_tool_names()) == len(set(ToolName.get_all_tool_names()))
