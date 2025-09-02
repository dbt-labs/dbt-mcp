import logging
import time
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

from dbtlabs_vortex.producer import shutdown
from dbtsl.client.sync import SyncSemanticLayerClient
from mcp.server.fastmcp import FastMCP
from mcp.types import ContentBlock, TextContent

from dbt_mcp.config.config import Config
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.dbt_admin.tools import register_admin_api_tools
from dbt_mcp.dbt_cli.tools import register_dbt_cli_tools
from dbt_mcp.discovery.tools import register_discovery_tools
from dbt_mcp.semantic_layer.tools import register_sl_tools
from dbt_mcp.sql.tools import SqlToolsManager, register_sql_tools
from dbt_mcp.tools.config import DbtMcpContext
from dbt_mcp.tracking.tracking import UsageTracker

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
        try:
            await SqlToolsManager.close()
        except Exception:
            logger.exception("Error closing SQL tools manager")
        try:
            shutdown()
        except Exception:
            logger.exception("Error shutting down MCP server")


class DbtMCP(FastMCP):
    def __init__(
        self,
        config: Config,
        usage_tracker: UsageTracker,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
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

    def get_context(self) -> DbtMcpContext:
        """
        Returns a Context object. Note that the context will only be valid
        during a request; outside a request, most methods will error.
        """
        try:
            request_context = self._mcp_server.request_context
        except LookupError:
            request_context = None
        return DbtMcpContext(
            request_context=request_context,
            fastmcp=self,
            semantic_layer_config=self.config.semantic_layer_config,
            semantic_layer_client=(
                SyncSemanticLayerClient(
                    environment_id=self.config.semantic_layer_config.prod_environment_id,
                    auth_token=self.config.semantic_layer_config.service_token,
                    host=self.config.semantic_layer_config.host,
                )
                if self.config.semantic_layer_config
                else None
            ),
            discovery_config=self.config.discovery_config,
            dbt_cli_config=self.config.dbt_cli_config,
            admin_api_config=self.config.admin_api_config,
            admin_api_client=(
                DbtAdminAPIClient(self.config.admin_api_config)
                if self.config.admin_api_config
                else None
            ),
        )


async def create_dbt_mcp(config: Config):
    dbt_mcp = DbtMCP(
        config=config,
        usage_tracker=UsageTracker(),
        name="dbt",
        lifespan=app_lifespan,
    )

    if config.semantic_layer_config:
        logger.info("Registering semantic layer tools")
        register_sl_tools(dbt_mcp, config.disable_tools)

    if config.discovery_config:
        logger.info("Registering discovery tools")
        register_discovery_tools(dbt_mcp, config.disable_tools)

    if config.dbt_cli_config:
        logger.info("Registering dbt cli tools")
        register_dbt_cli_tools(dbt_mcp, config.disable_tools)

    if config.admin_api_config:
        logger.info("Registering dbt admin API tools")
        register_admin_api_tools(dbt_mcp, config.disable_tools)

    if config.sql_config:
        logger.info("Registering SQL tools")
        await register_sql_tools(dbt_mcp, config.sql_config, config.disable_tools)

    return dbt_mcp
