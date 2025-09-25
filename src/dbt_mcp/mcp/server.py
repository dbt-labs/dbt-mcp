import logging
import time
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import (
    AbstractAsyncContextManager,
    asynccontextmanager,
)
from typing import Any

from dbtlabs_vortex.producer import shutdown
from mcp.server.fastmcp import FastMCP
from mcp.server.lowlevel.server import LifespanResultT
from mcp.types import (
    ContentBlock,
    TextContent,
)

from dbt_mcp.config.config import Config
from dbt_mcp.dbt_admin.tools import register_admin_api_tools
from dbt_mcp.dbt_cli.tools import register_dbt_cli_tools
from dbt_mcp.dbt_codegen.tools import register_dbt_codegen_tools
from dbt_mcp.discovery.tools import register_discovery_tools
from dbt_mcp.semantic_layer.client import DefaultSemanticLayerClientProvider
from dbt_mcp.semantic_layer.tools import register_sl_tools
from dbt_mcp.sql.tools import SqlToolsManager, register_sql_tools
from dbt_mcp.tracking.tracking import UsageTracker

logger = logging.getLogger(__name__)


class DbtMCP(FastMCP):
    def __init__(
        self,
        config: Config,
        usage_tracker: UsageTracker,
        lifespan: Callable[["DbtMCP"], AbstractAsyncContextManager[LifespanResultT]],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs, lifespan=lifespan)
        self.usage_tracker = usage_tracker
        self.config = config

    async def call_tool(
        self, name: str, arguments: dict[str, Any]
    ) -> Sequence[ContentBlock] | dict[str, Any]:
        logger.info(f"Calling tool: {name}")
        result = None
        start_time = int(time.time() * 1000)
        try:
            result = await super().call_tool(
                name,
                arguments,
            )
        except Exception as e:
            end_time = int(time.time() * 1000)
            logger.error(
                f"Error calling tool: {name} with arguments: {arguments} "
                + f"in {end_time - start_time}ms: {e}"
            )
            self.usage_tracker.emit_tool_called_event(
                config=self.config.tracking_config,
                tool_name=name,
                arguments=arguments,
                start_time_ms=start_time,
                end_time_ms=end_time,
                error_message=str(e),
            )
            return [
                TextContent(
                    type="text",
                    text=str(e),
                )
            ]
        end_time = int(time.time() * 1000)
        logger.info(f"Tool {name} called successfully in {end_time - start_time}ms")
        self.usage_tracker.emit_tool_called_event(
            config=self.config.tracking_config,
            tool_name=name,
            arguments=arguments,
            start_time_ms=start_time,
            end_time_ms=end_time,
            error_message=None,
        )
        return result


@asynccontextmanager
async def app_lifespan(server: DbtMCP) -> AsyncIterator[None]:
    logger.info("Starting MCP server")
    try:
        yield
    except Exception as e:
        logger.error(f"Error in MCP server: {e}")
        raise e
    finally:
        logger.info("Shutting down MCP server")
        try:
            await SqlToolsManager.close()
        except Exception:
            logger.exception("Error closing SQL tools manager")
        try:
            shutdown()
        except Exception:
            logger.exception("Error shutting down MCP server")


async def create_dbt_mcp(config: Config) -> DbtMCP:
    dbt_mcp = DbtMCP(
        config=config,
        usage_tracker=UsageTracker(),
        name="dbt",
        lifespan=app_lifespan,
    )

    if config.semantic_layer_config_provider:
        logger.info("Registering semantic layer tools")
        register_sl_tools(
            dbt_mcp,
            config_provider=config.semantic_layer_config_provider,
            client_provider=DefaultSemanticLayerClientProvider(
                config_provider=config.semantic_layer_config_provider,
            ),
            exclude_tools=config.disable_tools,
        )

    if config.discovery_config_provider:
        logger.info("Registering discovery tools")
        register_discovery_tools(
            dbt_mcp, config.discovery_config_provider, config.disable_tools
        )

    if config.dbt_cli_config:
        logger.info("Registering dbt cli tools")
        register_dbt_cli_tools(dbt_mcp, config.dbt_cli_config, config.disable_tools)

    if config.dbt_codegen_config:
        logger.info("Registering dbt codegen tools")
        register_dbt_codegen_tools(
            dbt_mcp, config.dbt_codegen_config, config.disable_tools
        )

    if config.admin_api_config_provider:
        logger.info("Registering dbt admin API tools")
        register_admin_api_tools(
            dbt_mcp, config.admin_api_config_provider, config.disable_tools
        )

    if config.sql_config_provider:
        logger.info("Registering SQL tools")
        await register_sql_tools(
            dbt_mcp, config.sql_config_provider, config.disable_tools
        )

    return dbt_mcp
