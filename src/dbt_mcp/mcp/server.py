import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import load_config
from dbt_mcp.dbt_cli.tools import register_dbt_cli_tools
from dbt_mcp.discovery.tools import register_discovery_tools
from dbt_mcp.remote.tools import register_remote_tools
from dbt_mcp.semantic_layer.tools import register_sl_tools

config = load_config()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[None]:
    logger.info("Starting MCP server")
    try:
        yield
    except Exception as e:
        logger.error(f"Error in MCP server: {e}")
        raise e
    finally:
        logger.info("Shutting down MCP server")


dbt_mcp = FastMCP("dbt", lifespan=app_lifespan)

if config.semantic_layer_config:
    register_sl_tools(dbt_mcp, config.semantic_layer_config)

if config.discovery_config:
    register_discovery_tools(dbt_mcp, config.discovery_config)

if config.dbt_cli_config:
    register_dbt_cli_tools(dbt_mcp, config.dbt_cli_config)

if config.remote_config:
    asyncio.run(register_remote_tools(dbt_mcp, config.remote_config))
