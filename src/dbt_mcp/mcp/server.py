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
from mcp.types import ContentBlock, TextContent, Tool

from dbt_mcp.config.config import Config
from dbt_mcp.dbt_admin.tools import register_admin_api_tools
from dbt_mcp.dbt_cli.tools import register_dbt_cli_tools
from dbt_mcp.dbt_codegen.tools import register_dbt_codegen_tools
from dbt_mcp.discovery.tools import register_discovery_tools
from dbt_mcp.discovery.tools_multiproject import register_multiproject_discovery_tools
from dbt_mcp.errors.common import MissingHostError
from dbt_mcp.lsp.providers.lsp_connection_provider import LSPConnectionProviderProtocol
from dbt_mcp.lsp.tools import register_lsp_tools
from dbt_mcp.mcp_server_metadata.tools import register_mcp_server_tools
from dbt_mcp.product_docs.tools import register_product_docs_tools
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.proxy.tools import ProxiedToolsManager, register_proxied_tools
from dbt_mcp.semantic_layer.client import DefaultSemanticLayerClientProvider
from dbt_mcp.semantic_layer.tools import register_sl_tools
from dbt_mcp.semantic_layer.tools_multiproject import register_multiproject_sl_tools
from dbt_mcp.tracking.tracking import DefaultUsageTracker, ToolCalledEvent, UsageTracker

logger = logging.getLogger(__name__)

_REDACT_ARGS: frozenset[str] = frozenset({"sql_query", "vars"})


def _safe_args(arguments: dict[str, Any]) -> dict[str, Any]:
    return {k: "***" if k in _REDACT_ARGS else v for k, v in arguments.items()}


class DbtMCP(FastMCP):
    def __init__(
        self,
        config: Config,
        multi_project_mcp: FastMCP,
        single_project_mcp: FastMCP,
        usage_tracker: UsageTracker,
        lifespan: (
            Callable[
                [FastMCP[LifespanResultT]], AbstractAsyncContextManager[LifespanResultT]
            ]
            | None
        ),
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs, lifespan=lifespan)
        self.usage_tracker = usage_tracker
        self.config = config
        self.multi_project_mcp = multi_project_mcp
        self.single_project_mcp = single_project_mcp
        self._lsp_connection_task: (
            asyncio.Task[LSPConnectionProviderProtocol] | None
        ) = None

    async def _is_multi_project(self) -> bool:
        try:
            (
                settings,
                _,
            ) = await self.config.credentials_provider.get_credentials()
        except MissingHostError as e:
            logger.warning(
                "Could not resolve credentials — defaulting to single-project mode: %s",
                e,
            )
            return False
        return bool(
            settings.dbt_project_ids is not None and len(settings.dbt_project_ids) > 0
        )

    async def call_tool(
        self, name: str, arguments: dict[str, Any]
    ) -> Sequence[ContentBlock] | dict[str, Any]:
        logger.info(f"Calling tool: {name} with arguments: {_safe_args(arguments)}")
        result = None
        start_time = int(time.time() * 1000)
        try:
            if await self._is_multi_project():
                result = await self.multi_project_mcp.call_tool(name, arguments)
            else:
                result = await self.single_project_mcp.call_tool(name, arguments)
        except Exception as e:
            end_time = int(time.time() * 1000)
            logger.error(
                f"Error calling tool: {name} with arguments: {_safe_args(arguments)} "
                + f"in {end_time - start_time}ms: {e}"
            )
            try:
                await self.usage_tracker.emit_tool_called_event(
                    tool_called_event=ToolCalledEvent(
                        tool_name=name,
                        arguments=arguments,
                        start_time_ms=start_time,
                        end_time_ms=end_time,
                        error_message=str(e),
                    ),
                )
            except Exception:
                logger.debug("Usage tracking failed — skipping", exc_info=True)
            return [
                TextContent(
                    type="text",
                    text=str(e),
                )
            ]
        end_time = int(time.time() * 1000)
        logger.info(f"Tool {name} called successfully in {end_time - start_time}ms")
        try:
            await self.usage_tracker.emit_tool_called_event(
                tool_called_event=ToolCalledEvent(
                    tool_name=name,
                    arguments=arguments,
                    start_time_ms=start_time,
                    end_time_ms=end_time,
                    error_message=None,
                ),
            )
        except Exception:
            logger.debug("Usage tracking failed — skipping", exc_info=True)
        return result

    async def list_tools(self) -> list[Tool]:
        if await self._is_multi_project():
            return await self.multi_project_mcp.list_tools()
        return await self.single_project_mcp.list_tools()


@asynccontextmanager
async def app_lifespan(server: FastMCP[Any]) -> AsyncIterator[bool | None]:
    if not isinstance(server, DbtMCP):
        raise TypeError("app_lifespan can only be used with DbtMCP servers")
    logger.info("Starting MCP server")
    try:
        # register proxied tools inside the app lifespan to ensure the StreamableHTTP client (specific
        # to dbt Platform connection) lives on the same event loop as the running server
        # this avoids anyio cancel scope violations (see issue #498)
        if (
            server.config.proxied_tool_config_provider
            and not await server._is_multi_project()
        ):
            try:
                logger.info("Registering proxied tools")
                await register_proxied_tools(
                    dbt_mcp=server.single_project_mcp,
                    config_provider=server.config.proxied_tool_config_provider,
                    disabled_tools=set(server.config.disable_tools),
                    enabled_tools=(
                        set(server.config.enable_tools)
                        if server.config.enable_tools is not None
                        else None
                    ),
                    enabled_toolsets=server.config.enabled_toolsets,
                    disabled_toolsets=server.config.disabled_toolsets,
                )
            except MissingHostError as e:
                logger.warning(
                    "Could not register proxied tools — credentials not yet available: %s",
                    e,
                )

        # eager start and initialize the LSP connection
        if server.config.lsp_config:
            asyncio.create_task(
                server.config.lsp_config.local_lsp_connection_provider.get_connection()
            )
        yield None
    except Exception as e:
        logger.exception(f"Error in MCP server: {e}")
        raise
    finally:
        logger.info("Shutting down MCP server")
        try:
            await ProxiedToolsManager.close()
        except Exception:
            logger.exception("Error closing proxied tools manager")
        try:
            if server.config.lsp_config:
                await (
                    server.config.lsp_config.local_lsp_connection_provider
                ).cleanup_connection()
        except Exception:
            logger.exception("Error cleaning up LSP connection")
        try:
            shutdown()
        except Exception:
            logger.exception("Error shutting down MCP server")


async def register_multi_project_dbt_mcp(dbt_mcp: FastMCP, config: Config) -> None:
    disabled_tools = set(config.disable_tools)
    enabled_tools = (
        set(config.enable_tools) if config.enable_tools is not None else None
    )
    enabled_toolsets = config.enabled_toolsets
    disabled_toolsets = config.disabled_toolsets

    logger.info("Registering semantic layer tools for multi-project")
    register_multiproject_sl_tools(
        dbt_mcp=dbt_mcp,
        config_provider=config.multi_project_semantic_layer_config_provider,
        client_provider=DefaultSemanticLayerClientProvider(),
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )

    logger.info("Registering discovery tools for multi-project")
    register_multiproject_discovery_tools(
        dbt_mcp=dbt_mcp,
        config_provider=config.multi_project_discovery_config_provider,
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )

    if config.admin_api_config_provider:
        logger.info("Registering dbt admin API tools for multi-project")
        register_admin_api_tools(
            dbt_mcp,
            config.admin_api_config_provider,
            disabled_tools=disabled_tools,
            enabled_tools=enabled_tools,
            enabled_toolsets=enabled_toolsets,
            disabled_toolsets=disabled_toolsets,
        )


async def register_dbt_mcp_tools(dbt_mcp: FastMCP, config: Config) -> None:
    disabled_tools = set(config.disable_tools)
    enabled_tools = (
        set(config.enable_tools) if config.enable_tools is not None else None
    )
    enabled_toolsets = config.enabled_toolsets
    disabled_toolsets = config.disabled_toolsets

    # Register product docs tools (always available, fetches from public docs.getdbt.com)
    logger.info("Registering product docs tools")
    register_product_docs_tools(
        dbt_mcp,
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )

    # Register MCP server tools (always available)
    logger.info("Registering MCP server tools")
    register_mcp_server_tools(
        dbt_mcp,
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )

    logger.info("Registering semantic layer tools")
    register_sl_tools(
        dbt_mcp,
        config_provider=config.semantic_layer_config_provider,
        client_provider=DefaultSemanticLayerClientProvider(),
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )

    logger.info("Registering discovery tools")
    register_discovery_tools(
        dbt_mcp,
        discovery_config_provider=config.discovery_config_provider,
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

    logger.info("Registering dbt admin API tools")
    register_admin_api_tools(
        dbt_mcp,
        config.admin_api_config_provider,
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )

    if config.lsp_config:
        logger.info("Registering LSP tools")
        await register_lsp_tools(
            dbt_mcp,
            config.lsp_config.lsp_client_provider,
            disabled_tools=disabled_tools,
            enabled_tools=enabled_tools,
            enabled_toolsets=enabled_toolsets,
            disabled_toolsets=disabled_toolsets,
        )


async def create_dbt_mcp(config: Config) -> FastMCP:
    multi_project_dbt_mcp = FastMCP()
    await register_multi_project_dbt_mcp(multi_project_dbt_mcp, config)

    single_project_dbt_mcp = FastMCP()
    await register_dbt_mcp_tools(single_project_dbt_mcp, config)

    tool_dispatcher = DbtMCP(
        name="dbt",
        instructions=get_prompt("mcp/server_instructions").strip(),
        config=config,
        usage_tracker=DefaultUsageTracker(
            credentials_provider=config.credentials_provider.inner_provider,
            session_id=uuid.uuid4(),
        ),
        lifespan=app_lifespan,
        multi_project_mcp=multi_project_dbt_mcp,
        single_project_mcp=single_project_dbt_mcp,
    )
    return tool_dispatcher
