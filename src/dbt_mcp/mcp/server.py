import asyncio
import logging
import time
import uuid
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

from dbtlabs_vortex.producer import shutdown
from mcp.server.fastmcp import FastMCP
from mcp.server.lowlevel.server import LifespanResultT
from mcp.types import ContentBlock, TextContent

from dbt_mcp.config.config import Config
from dbt_mcp.dbt_admin.tools import register_admin_api_tools
from dbt_mcp.dbt_cli.tools import register_dbt_cli_tools
from dbt_mcp.dbt_codegen.tools import register_dbt_codegen_tools
from dbt_mcp.discovery.tools import register_discovery_tools
from dbt_mcp.lsp.providers.local_lsp_client_provider import LocalLSPClientProvider
from dbt_mcp.lsp.providers.local_lsp_connection_provider import (
    LocalLSPConnectionProvider,
)
from dbt_mcp.lsp.providers.lsp_connection_provider import LSPConnectionProviderProtocol
from dbt_mcp.lsp.tools import register_lsp_tools
from dbt_mcp.proxy.tools import ProxiedToolsManager, register_proxied_tools
from dbt_mcp.semantic_layer.client import DefaultSemanticLayerClientProvider
from dbt_mcp.semantic_layer.tools import register_sl_tools
from dbt_mcp.tracking.tracking import DefaultUsageTracker, ToolCalledEvent, UsageTracker

logger = logging.getLogger(__name__)


class DbtMCP(FastMCP):
    def __init__(
        self,
        config: Config,
        usage_tracker: UsageTracker,
        lifespan: (
            Callable[
                [FastMCP[LifespanResultT]], AbstractAsyncContextManager[LifespanResultT]
            ]
            | None
        ),
        lsp_connection_provider: LocalLSPConnectionProvider | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs, lifespan=lifespan)
        self.usage_tracker = usage_tracker
        self.config = config
        self.lsp_connection_provider = lsp_connection_provider
        self._lsp_connection_task: (
            asyncio.Task[LSPConnectionProviderProtocol] | None
        ) = None

    async def call_tool(
        self, name: str, arguments: dict[str, Any]
    ) -> Sequence[ContentBlock] | dict[str, Any]:
        logger.info(f"Calling tool: {name} with arguments: {arguments}")
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
            await self.usage_tracker.emit_tool_called_event(
                tool_called_event=ToolCalledEvent(
                    tool_name=name,
                    arguments=arguments,
                    start_time_ms=start_time,
                    end_time_ms=end_time,
                    error_message=str(e),
                ),
            )
            return [
                TextContent(
                    type="text",
                    text=str(e),
                )
            ]
        end_time = int(time.time() * 1000)
        logger.info(f"Tool {name} called successfully in {end_time - start_time}ms")
        await self.usage_tracker.emit_tool_called_event(
            tool_called_event=ToolCalledEvent(
                tool_name=name,
                arguments=arguments,
                start_time_ms=start_time,
                end_time_ms=end_time,
                error_message=None,
            ),
        )
        return result


@asynccontextmanager
async def app_lifespan(server: FastMCP[Any]) -> AsyncIterator[bool | None]:
    if not isinstance(server, DbtMCP):
        raise TypeError("app_lifespan can only be used with DbtMCP servers")
    logger.info("Starting MCP server")
    try:
        # eager start and initialize the LSP connection
        if server.lsp_connection_provider:
            asyncio.create_task(server.lsp_connection_provider.get_connection())
        yield None
    except Exception as e:
        logger.error(f"Error in MCP server: {e}")
        raise e
    finally:
        logger.info("Shutting down MCP server")
        try:
            await ProxiedToolsManager.close()
        except Exception:
            logger.exception("Error closing proxied tools manager")
        try:
            if server.lsp_connection_provider:
                await server.lsp_connection_provider.cleanup_connection()
        except Exception:
            logger.exception("Error cleaning up LSP connection")
        try:
            shutdown()
        except Exception:
            logger.exception("Error shutting down MCP server")


async def create_dbt_mcp(config: Config) -> DbtMCP:
    dbt_mcp = DbtMCP(
        config=config,
        usage_tracker=DefaultUsageTracker(
            credentials_provider=config.credentials_provider,
            session_id=uuid.uuid4(),
        ),
        name="dbt",
        lifespan=app_lifespan,
    )
    disabled_tools = set(config.disable_tools)
    enabled_tools = set(config.enable_tools)
    enabled_toolsets = config.enabled_toolsets
    disabled_toolsets = config.disabled_toolsets

    if config.semantic_layer_config_provider:
        logger.info("Registering semantic layer tools")
        register_sl_tools(
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

    if config.discovery_config_provider:
        logger.info("Registering discovery tools")
        register_discovery_tools(
            dbt_mcp,
            config.discovery_config_provider,
            disabled_tools=disabled_tools,
            enabled_tools=enabled_tools,
            enabled_toolsets=enabled_toolsets,
            disabled_toolsets=disabled_toolsets,
        )

    if config.dbt_cli_config:
        logger.info("Registering dbt cli tools")
        register_dbt_cli_tools(
            dbt_mcp,
            config=config.dbt_cli_config,
            disabled_tools=disabled_tools,
            enabled_tools=enabled_tools,
            enabled_toolsets=enabled_toolsets,
            disabled_toolsets=disabled_toolsets,
        )

    if config.dbt_codegen_config:
        logger.info("Registering dbt codegen tools")
        register_dbt_codegen_tools(
            dbt_mcp,
            config=config.dbt_codegen_config,
            disabled_tools=disabled_tools,
            enabled_tools=enabled_tools,
            enabled_toolsets=enabled_toolsets,
            disabled_toolsets=disabled_toolsets,
        )

    if config.admin_api_config_provider:
        logger.info("Registering dbt admin API tools")
        register_admin_api_tools(
            dbt_mcp,
            config.admin_api_config_provider,
            disabled_tools=disabled_tools,
            enabled_tools=enabled_tools,
            enabled_toolsets=enabled_toolsets,
            disabled_toolsets=disabled_toolsets,
        )

    if config.proxied_tool_config_provider:
        logger.info("Registering proxied tools")
        await register_proxied_tools(
            dbt_mcp=dbt_mcp,
            config_provider=config.proxied_tool_config_provider,
            disabled_tools=disabled_tools,
            enabled_tools=enabled_tools,
            enabled_toolsets=enabled_toolsets,
            disabled_toolsets=disabled_toolsets,
        )

    if config.lsp_config and config.lsp_config.lsp_binary_info:
        logger.info("Registering LSP tools")
        local_lsp_connection_provider = LocalLSPConnectionProvider(
            lsp_binary_info=config.lsp_config.lsp_binary_info,
            project_dir=config.lsp_config.project_dir,
        )
        lsp_client_provider = LocalLSPClientProvider(
            lsp_connection_provider=local_lsp_connection_provider,
        )
        dbt_mcp.lsp_connection_provider = local_lsp_connection_provider
        await register_lsp_tools(
            dbt_mcp,
            lsp_client_provider,
            disabled_tools=disabled_tools,
            enabled_tools=enabled_tools,
            enabled_toolsets=enabled_toolsets,
            disabled_toolsets=disabled_toolsets,
        )

    return dbt_mcp
