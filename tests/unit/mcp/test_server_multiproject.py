"""Tests for the multi-project MCP server (Server B)."""

import os
from unittest.mock import patch

from dbt_mcp.config.config import load_config
from dbt_mcp.dbt_cli.binary_type import BinaryType
from dbt_mcp.lsp.lsp_binary_manager import LspBinaryInfo
from dbt_mcp.mcp.server_multiproject import create_dbt_mcp_multiproject
from tests.mocks.config import mock_config


async def test_server_b_registers_list_projects():
    """Server B always registers list_projects via admin API tools."""
    dbt_mcp = await create_dbt_mcp_multiproject(mock_config)
    tool_names = {tool.name for tool in await dbt_mcp.list_tools()}
    assert "list_projects" in tool_names


async def test_server_b_registers_multiproject_semantic_layer_tools():
    """Server B registers all 6 multi-project semantic layer tools (same names as single-project)."""
    dbt_mcp = await create_dbt_mcp_multiproject(mock_config)
    tool_names = {tool.name for tool in await dbt_mcp.list_tools()}
    expected = {
        "list_metrics",
        "list_saved_queries",
        "get_dimensions",
        "get_entities",
        "query_metrics",
        "get_metrics_compiled_sql",
    }
    assert expected <= tool_names


async def test_server_b_registers_sql_for_project_tools():
    """Server B registers text_to_sql and execute_sql (same names, with project_id param)."""
    dbt_mcp = await create_dbt_mcp_multiproject(mock_config)
    tool_names = {tool.name for tool in await dbt_mcp.list_tools()}
    assert "text_to_sql" in tool_names
    assert "execute_sql" in tool_names


async def test_server_b_registers_admin_for_project_tools():
    """Server B registers list_jobs and list_jobs_runs (same names, with project_id param)."""
    dbt_mcp = await create_dbt_mcp_multiproject(mock_config)
    tool_names = {tool.name for tool in await dbt_mcp.list_tools()}
    assert "list_jobs" in tool_names
    assert "list_jobs_runs" in tool_names


async def test_server_b_registers_discovery_tools():
    """Server B registers the 18 multi-project discovery tools."""
    dbt_mcp = await create_dbt_mcp_multiproject(mock_config)
    tool_names = {tool.name for tool in await dbt_mcp.list_tools()}
    expected_discovery_tools = {
        "get_mart_models",
        "get_all_models",
        "get_model_details",
        "get_model_parents",
        "get_model_children",
        "get_model_health",
        "get_model_performance",
        "get_lineage",
        "get_exposures",
        "get_exposure_details",
        "get_all_sources",
        "get_source_details",
        "get_all_macros",
        "get_macro_details",
        "get_seed_details",
        "get_semantic_model_details",
        "get_snapshot_details",
        "get_test_details",
    }
    assert expected_discovery_tools <= tool_names


async def test_server_b_semantic_layer_tools_have_project_id_param():
    """Server B semantic layer tools use the same names but include project_id parameter."""
    dbt_mcp = await create_dbt_mcp_multiproject(mock_config)
    tools = await dbt_mcp.list_tools()
    sl_tool_names = {
        "list_metrics",
        "list_saved_queries",
        "get_dimensions",
        "get_entities",
        "query_metrics",
        "get_metrics_compiled_sql",
    }
    for tool in tools:
        if tool.name in sl_tool_names:
            assert "project_id" in tool.inputSchema.get("properties", {}), (
                f"Tool {tool.name} should have project_id parameter in Server B"
            )


async def test_server_a_not_affected_by_multiproject_flag(env_setup):
    """When DBT_MCP_MULTI_PROJECT_ENABLED is not set, Server A is used."""
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
        # Ensure the env var is not set
        assert os.environ.get("DBT_MCP_MULTI_PROJECT_ENABLED") is None
        config = load_config(enable_proxied_tools=False)
        from dbt_mcp.mcp.server import create_dbt_mcp

        server_a = await create_dbt_mcp(config)
        tool_names = {tool.name for tool in await server_a.list_tools()}

        # Server A registers single-project admin tools (including list_projects)
        assert "list_projects" in tool_names
        # Server A registers single-project semantic layer tools (without project_id)
        assert "list_metrics" in tool_names


async def test_server_b_skips_discovery_when_no_config():
    """When discovery_config_provider is None, discovery tools are not registered."""
    from dbt_mcp.config.config import Config
    from tests.mocks.config import (
        MockAdminApiConfigProvider,
        MockCredentialsProvider,
        MockProxiedToolConfigProvider,
        MockSemanticLayerConfigProvider,
        mock_dbt_cli_config,
        mock_dbt_codegen_config,
        mock_lsp_config,
    )

    config_no_discovery = Config(
        proxied_tool_config_provider=MockProxiedToolConfigProvider(),
        dbt_cli_config=mock_dbt_cli_config,
        dbt_codegen_config=mock_dbt_codegen_config,
        discovery_config_provider=None,  # No discovery
        semantic_layer_config_provider=MockSemanticLayerConfigProvider(),
        admin_api_config_provider=MockAdminApiConfigProvider(),
        lsp_config=mock_lsp_config,
        disable_tools=[],
        enable_tools=None,
        disabled_toolsets=set(),
        enabled_toolsets=set(),
        credentials_provider=MockCredentialsProvider(),
    )

    dbt_mcp = await create_dbt_mcp_multiproject(config_no_discovery)
    tool_names = {tool.name for tool in await dbt_mcp.list_tools()}

    # Discovery tools should not be present
    assert "get_mart_models" not in tool_names
    assert "get_all_models" not in tool_names

    # But admin tools (including list_projects) should still be present
    assert "list_projects" in tool_names


async def test_server_b_skips_sql_when_no_proxied_tool_config():
    """When proxied_tool_config_provider is None, SQL for project tools are not registered."""
    from dbt_mcp.config.config import Config
    from tests.mocks.config import (
        MockAdminApiConfigProvider,
        MockCredentialsProvider,
        MockDiscoveryConfigProvider,
        MockSemanticLayerConfigProvider,
        mock_dbt_cli_config,
        mock_dbt_codegen_config,
        mock_lsp_config,
    )

    config_no_proxied = Config(
        proxied_tool_config_provider=None,  # No SQL tools
        dbt_cli_config=mock_dbt_cli_config,
        dbt_codegen_config=mock_dbt_codegen_config,
        discovery_config_provider=MockDiscoveryConfigProvider(),
        semantic_layer_config_provider=MockSemanticLayerConfigProvider(),
        admin_api_config_provider=MockAdminApiConfigProvider(),
        lsp_config=mock_lsp_config,
        disable_tools=[],
        enable_tools=None,
        disabled_toolsets=set(),
        enabled_toolsets=set(),
        credentials_provider=MockCredentialsProvider(),
    )

    dbt_mcp = await create_dbt_mcp_multiproject(config_no_proxied)
    tool_names = {tool.name for tool in await dbt_mcp.list_tools()}

    assert "text_to_sql" not in tool_names
    assert "execute_sql" not in tool_names


async def test_server_b_registers_product_docs_tools():
    """Server B registers product docs tools (always available)."""
    dbt_mcp = await create_dbt_mcp_multiproject(mock_config)
    tool_names = {tool.name for tool in await dbt_mcp.list_tools()}
    assert "search_product_docs" in tool_names
    assert "get_product_doc_pages" in tool_names


async def test_server_b_registers_mcp_server_metadata_tools():
    """Server B registers MCP server metadata tools (always available)."""
    dbt_mcp = await create_dbt_mcp_multiproject(mock_config)
    tool_names = {tool.name for tool in await dbt_mcp.list_tools()}
    assert "get_mcp_server_version" in tool_names


async def test_server_b_skips_admin_when_no_config():
    """When admin_api_config_provider is None, admin tools are not registered."""
    from dbt_mcp.config.config import Config
    from tests.mocks.config import (
        MockCredentialsProvider,
        MockDiscoveryConfigProvider,
        MockProxiedToolConfigProvider,
        MockSemanticLayerConfigProvider,
        mock_dbt_cli_config,
        mock_dbt_codegen_config,
        mock_lsp_config,
    )

    config_no_admin = Config(
        proxied_tool_config_provider=MockProxiedToolConfigProvider(),
        dbt_cli_config=mock_dbt_cli_config,
        dbt_codegen_config=mock_dbt_codegen_config,
        discovery_config_provider=MockDiscoveryConfigProvider(),
        semantic_layer_config_provider=MockSemanticLayerConfigProvider(),
        admin_api_config_provider=None,
        lsp_config=mock_lsp_config,
        disable_tools=[],
        enable_tools=None,
        disabled_toolsets=set(),
        enabled_toolsets=set(),
        credentials_provider=MockCredentialsProvider(),
    )

    dbt_mcp = await create_dbt_mcp_multiproject(config_no_admin)
    tool_names = {tool.name for tool in await dbt_mcp.list_tools()}

    assert "list_jobs" not in tool_names
    assert "list_jobs_runs" not in tool_names
    # When admin is None, list_projects is also not present
    assert "list_projects" not in tool_names
