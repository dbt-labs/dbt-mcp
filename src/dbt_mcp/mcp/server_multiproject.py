import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import Config
from dbt_mcp.config.config_providers import DefaultSemanticLayerConfigProvider
from dbt_mcp.dbt_admin.tools import register_admin_api_tools
from dbt_mcp.discovery.tools_multiproject import register_multiproject_discovery_tools
from dbt_mcp.mcp.server import DbtMCP
from dbt_mcp.mcp_server_metadata.tools import register_mcp_server_tools
from dbt_mcp.product_docs.tools import register_product_docs_tools
from dbt_mcp.project.tools import register_project_tools
from dbt_mcp.semantic_layer.client import DefaultSemanticLayerClientProvider
from dbt_mcp.semantic_layer.tools_multiproject import register_multiproject_sl_tools
from dbt_mcp.sql.tools import register_sql_for_project_tools
from dbt_mcp.tracking.tracking import DefaultUsageTracker

logger = logging.getLogger(__name__)


@asynccontextmanager
async def multiproject_app_lifespan(
    server: FastMCP[Any],
) -> AsyncIterator[bool | None]:
    if not isinstance(server, DbtMCP):
        raise TypeError(
            "multiproject_app_lifespan can only be used with DbtMCP servers"
        )
    logger.info("Starting multi-project MCP server")
    try:
        yield None
    finally:
        logger.info("Shutting down multi-project MCP server")


async def create_dbt_mcp_multiproject(config: Config) -> DbtMCP:
    dbt_mcp = DbtMCP(
        config=config,
        usage_tracker=DefaultUsageTracker(
            credentials_provider=config.credentials_provider,
            session_id=uuid.uuid4(),
        ),
        name="dbt-multiproject",
        lifespan=multiproject_app_lifespan,
    )

    disabled_tools = set(config.disable_tools)
    enabled_tools = (
        set(config.enable_tools) if config.enable_tools is not None else None
    )
    enabled_toolsets = config.enabled_toolsets
    disabled_toolsets = config.disabled_toolsets

    # Register product docs and MCP server metadata tools (always available)
    logger.info("Registering product docs tools")
    register_product_docs_tools(
        dbt_mcp,
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )

    logger.info("Registering MCP server metadata tools")
    register_mcp_server_tools(
        dbt_mcp,
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )

    logger.info("Registering project discovery tools")
    register_project_tools(
        dbt_mcp,
        credentials_provider=config.credentials_provider,
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )

    if config.discovery_config_provider:
        logger.info("Registering multi-project discovery tools")
        register_multiproject_discovery_tools(
            dbt_mcp,
            discovery_config_provider=config.discovery_config_provider,
            disabled_tools=disabled_tools,
            enabled_tools=enabled_tools,
            enabled_toolsets=enabled_toolsets,
            disabled_toolsets=disabled_toolsets,
        )

    if config.semantic_layer_config_provider and isinstance(
        config.semantic_layer_config_provider, DefaultSemanticLayerConfigProvider
    ):
        logger.info("Registering multi-project semantic layer tools")
        register_multiproject_sl_tools(
            dbt_mcp,
            config_provider=config.semantic_layer_config_provider,
            client_provider=DefaultSemanticLayerClientProvider(
                config_provider=config.semantic_layer_config_provider,
            ),
            disabled_tools=disabled_tools,
            enabled_tools=enabled_tools,
            enabled_toolsets=enabled_toolsets,
            disabled_toolsets=disabled_toolsets,
        )

    if config.proxied_tool_config_provider:
        logger.info("Registering multi-project SQL tools")
        register_sql_for_project_tools(
            dbt_mcp,
            proxied_tool_config_provider=config.proxied_tool_config_provider,
            disabled_tools=disabled_tools,
            enabled_tools=enabled_tools,
            enabled_toolsets=enabled_toolsets,
            disabled_toolsets=disabled_toolsets,
        )

    if config.admin_api_config_provider:
        logger.info("Registering multi-project admin API tools")
        register_admin_api_tools(
            dbt_mcp,
            config.admin_api_config_provider,
            disabled_tools=disabled_tools,
            enabled_tools=enabled_tools,
            enabled_toolsets=enabled_toolsets,
            disabled_toolsets=disabled_toolsets,
        )

    return dbt_mcp
