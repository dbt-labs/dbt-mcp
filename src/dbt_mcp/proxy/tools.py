import logging
from collections.abc import Sequence
from contextlib import AsyncExitStack
from typing import Annotated, Any

from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from mcp import ClientSession
from mcp.client.streamable_http import GetSessionIdCallback, streamable_http_client
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.tools.base import Tool as InternalTool
from mcp.server.fastmcp.utilities.func_metadata import (
    ArgModelBase,
    FuncMetadata,
)
from mcp.shared.message import SessionMessage
from mcp.types import (
    ContentBlock,
    TextContent,
    Tool,
)
from pydantic import Field, WithJsonSchema, create_model
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from dbt_mcp.config.config_providers import ConfigProvider, ProxiedToolConfig
from dbt_mcp.errors import RemoteToolError
from dbt_mcp.errors.warehouse_auth import append_hint, is_warehouse_auth_error
from dbt_mcp.proxy.http_client import create_mcp_http_client
from dbt_mcp.tools.register import should_register_tool
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import TOOL_TO_TOOLSET, Toolset, proxied_tools

logger = logging.getLogger(__name__)


# Based on this: https://github.com/modelcontextprotocol/python-sdk/blob/9ae4df85fbab97bf476ddd160b766ca4c208cd13/src/mcp/server/fastmcp/utilities/func_metadata.py#L105
def get_remote_tool_fn_metadata(tool: Tool) -> FuncMetadata:
    dynamic_pydantic_model_params: dict[str, Any] = {}
    for key in tool.inputSchema["properties"]:
        # Remote tools shouldn't have type annotations or default values
        # for their arguments. So, we set them to defaults. The annotation is
        # always a concrete type (never a string forward reference), so no
        # type evaluation step is needed here.
        annotation: Any = Annotated[
            Any,
            Field(),
            WithJsonSchema({"title": key, "type": "string"}),
        ]
        field_info = FieldInfo.from_annotated_attribute(
            annotation=annotation,
            default=PydanticUndefined,
        )
        dynamic_pydantic_model_params[key] = (field_info.annotation, None)
    return FuncMetadata(
        arg_model=create_model(
            f"{tool.name}Arguments",
            **dynamic_pydantic_model_params,
            __base__=ArgModelBase,
        )
    )


async def get_proxied_tools(
    session: ClientSession,
    configured_proxied_tools: set[ToolName],
) -> list[Tool]:
    tools = (await session.list_tools()).tools
    normalized_configured_proxied_tools = {
        t.value.lower() for t in configured_proxied_tools
    }
    return [t for t in tools if t.name.lower() in normalized_configured_proxied_tools]


def _content_text(content: Sequence[ContentBlock]) -> str:
    """Readable text of tool result content, without the Python repr of the blocks."""
    return "\n".join(
        block.text if isinstance(block, TextContent) else str(block)
        for block in content
    )


async def format_remote_tool_error(
    tool_name: str, content: Sequence[ContentBlock], config: ProxiedToolConfig
) -> str:
    """Describe a remote tool failure, adding how to fix expired warehouse auth."""
    message = f"Tool {tool_name} reported an error: {_content_text(content)}"
    hint_provider = config.warehouse_auth_hint_provider
    if hint_provider is None or not is_warehouse_auth_error(message):
        return message
    try:
        # execute_sql runs as the configured developer, so prefer the dev environment
        hint = await hint_provider.get_hint(
            environment_id=config.dev_environment_id or config.prod_environment_id,
            developer_credentials=True,
            user_id=config.user_id,
        )
    except Exception:
        logger.warning("Could not build the warehouse auth hint", exc_info=True)
        return message
    return append_hint(message, hint)


class ProxiedToolsManager:
    _stack = AsyncExitStack()

    async def get_remote_mcp_session(
        self, url: str, headers: dict[str, str]
    ) -> ClientSession:
        http_client = await self._stack.enter_async_context(
            create_mcp_http_client(headers=headers)
        )
        streamable_http_client_context: tuple[
            MemoryObjectReceiveStream[SessionMessage | Exception],
            MemoryObjectSendStream[SessionMessage],
            GetSessionIdCallback,
        ] = await self._stack.enter_async_context(
            streamable_http_client(
                url=url,
                http_client=http_client,
            )
        )
        read_stream, write_stream, _ = streamable_http_client_context
        return await self._stack.enter_async_context(
            ClientSession(read_stream, write_stream)
        )

    @classmethod
    async def close(cls) -> None:
        await cls._stack.aclose()


async def register_proxied_tools(
    dbt_mcp: FastMCP,
    config_provider: ConfigProvider[ProxiedToolConfig],
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName] | None,
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    """
    Register proxied MCP tools.

    Proxied tools are hosted remotely, so their definitions aren't found in this repo.
    """
    configured_proxied_tools: set[ToolName] = {
        t
        for t in proxied_tools
        if should_register_tool(
            tool_name=t,
            enabled_tools=enabled_tools,
            disabled_tools=disabled_tools,
            enabled_toolsets=enabled_toolsets,
            disabled_toolsets=disabled_toolsets,
            tool_to_toolset=TOOL_TO_TOOLSET,
        )
    }
    if not configured_proxied_tools:
        return
    config = await config_provider.get_config()
    headers = config.headers_provider.get_headers()
    if config.prod_environment_id:
        headers["x-dbt-prod-environment-id"] = str(config.prod_environment_id)
    if config.dev_environment_id:
        headers["x-dbt-dev-environment-id"] = str(config.dev_environment_id)
    if config.user_id:
        headers["x-dbt-user-id"] = str(config.user_id)
    proxied_tools_manager = ProxiedToolsManager()
    try:
        session = await proxied_tools_manager.get_remote_mcp_session(
            config.url, headers
        )
        await session.initialize()
        tools = await get_proxied_tools(session, configured_proxied_tools)
    except BaseException as e:
        logger.error(f"Error getting proxied tools: {e}")
        try:
            await proxied_tools_manager.close()
        except Exception:
            logger.exception("Error closing proxied tools manager after failure")
        return
    logger.info(f"Loaded proxied tools: {', '.join([tool.name for tool in tools])}")
    for tool in tools:
        # Create a new function using a factory to avoid closure issues
        def create_tool_function(tool_name: str):
            async def tool_function(*args, **kwargs) -> Sequence[ContentBlock]:
                tool_call_result = await session.call_tool(
                    tool_name,
                    kwargs,
                )
                if tool_call_result.isError:
                    raise RemoteToolError(
                        await format_remote_tool_error(
                            tool_name, tool_call_result.content, config
                        )
                    )
                return tool_call_result.content

            return tool_function

        dbt_mcp._tool_manager._tools[tool.name] = InternalTool(
            fn=create_tool_function(tool.name),
            title=tool.title,
            name=tool.name,
            annotations=tool.annotations,
            description=tool.description or "",
            parameters=tool.inputSchema,
            fn_metadata=get_remote_tool_fn_metadata(tool),
            is_async=True,
            context_kwarg=None,
        )
