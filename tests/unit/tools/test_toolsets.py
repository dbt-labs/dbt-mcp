from unittest.mock import patch

from dbt_mcp.config.config import (
    TOOLSET_TO_DISABLE_ATTR,
    TOOLSET_TO_ENABLE_ATTR,
    load_config,
)
from dbt_mcp.dbt_cli.binary_type import BinaryType
from dbt_mcp.lsp.lsp_binary_manager import LspBinaryInfo
from dbt_mcp.mcp.server import create_dbt_mcp
from dbt_mcp.tools.toolsets import Toolset, proxied_tools, toolsets


def test_toolset_enable_disable_attr_cover_every_toolset() -> None:
    """Ensure each toolset has enable/disable flags."""
    expected_toolsets = set(Toolset)

    disable_keys = set(TOOLSET_TO_DISABLE_ATTR.keys())
    enable_keys = set(TOOLSET_TO_ENABLE_ATTR.keys())

    missing_disable = sorted(
        toolset.value for toolset in expected_toolsets - disable_keys
    )
    missing_enable = sorted(
        toolset.value for toolset in expected_toolsets - enable_keys
    )

    assert disable_keys == expected_toolsets, (
        f"Missing disable attrs for: {missing_disable}" if missing_disable else ""
    )
    assert enable_keys == expected_toolsets, (
        f"Missing enable attrs for: {missing_enable}" if missing_enable else ""
    )


async def test_toolsets_match_server_tools(env_setup):
    """Test that the defined toolsets match the tools registered in the server."""
    with (
        env_setup(
            env_vars={
                "DISABLE_DBT_CODEGEN": "false",
                "DISABLE_MCP_SERVER_METADATA": "false",
            }
        ),
        patch(
            "dbt_mcp.config.config.detect_binary_type", return_value=BinaryType.DBT_CORE
        ),
        patch(
            "dbt_mcp.config.config.dbt_lsp_binary_info",
            return_value=LspBinaryInfo(path="/path/to/lsp", version="1.0.0"),
        ),
    ):
        config = load_config(enable_proxied_tools=False)
        dbt_mcp = await create_dbt_mcp(config)

        # Get all tools from the server
        server_tools = await dbt_mcp.list_tools()
        # Manually adding SQL tools here because the server doesn't get them
        # in this unit test.
        server_tool_names = {tool.name for tool in server_tools} | {
            p.value for p in proxied_tools
        }
        defined_tools = set()
        for toolset_tools in toolsets.values():
            defined_tools.update({t.value for t in toolset_tools})

        if server_tool_names != defined_tools:
            raise ValueError(
                f"Tool name mismatch:\n"
                f"In server but not in enum: {server_tool_names - defined_tools}\n"
                f"In enum but not in server: {defined_tools - server_tool_names}"
            )
