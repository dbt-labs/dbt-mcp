import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import Config
from dbt_mcp.mcp.server import DbtMCP
from dbt_mcp.project.tools import register_project_tools
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

    logger.info("Registering project discovery tools")
    register_project_tools(
        dbt_mcp,
        credentials_provider=config.credentials_provider,
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )

    return dbt_mcp
