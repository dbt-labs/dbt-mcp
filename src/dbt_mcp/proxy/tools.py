import logging
from collections.abc import Sequence
from contextlib import AsyncExitStack
from typing import (
    Annotated,
    Any,
    ForwardRef,
)

from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from mcp import ClientSession
from mcp.client.streamable_http import GetSessionIdCallback, streamablehttp_client
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import InvalidSignature
from mcp.server.fastmcp.tools.base import Tool as InternalTool
from mcp.server.fastmcp.utilities.func_metadata import (
    ArgModelBase,
    FuncMetadata,
)
from mcp.shared.message import SessionMessage
from mcp.types import (
    ContentBlock,
    Tool,
)
from pydantic import Field, WithJsonSchema, create_model
from pydantic._internal._typing_extra import eval_type_backport
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from dbt_mcp.config.config_providers import ConfigProvider, ProxiedToolConfig
from dbt_mcp.errors import RemoteToolError
from dbt_mcp.tools.register import should_register_tool
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import TOOL_TO_TOOLSET, Toolset, proxied_tools

logger = logging.getLogger(__name__)


# Based on this: https://github.com/modelcontextprotocol/python-sdk/blob/9ae4df85fbab97bf476ddd160b766ca4c208cd13/src/mcp/server/fastmcp/utilities/func_metadata.py#L179
def _get_typed_annotation(annotation: Any, globalns: dict[str, Any]) -> Any:
    def try_eval_type(
        value: Any, globalns: dict[str, Any], localns: dict[str, Any]
    ) -> tuple[Any, bool]:
        try:
            return eval_type_backport(value, globalns, localns), True
        except NameError:
            return value, False

    if isinstance(annotation, str):
        annotation = ForwardRef(annotation)
        annotation, status = try_eval_type(annotation, globalns, globalns)

        # This check and raise could perhaps be skipped, and we (FastMCP) just call
        # model_rebuild right before using it 🤷
        if status is False:
            raise InvalidSignature(f"Unable to evaluate type annotation {annotation}")

    return annotation


# Based on this: https://github.com/modelcontextprotocol/python-sdk/blob/9ae4df85fbab97bf476ddd160b766ca4c208cd13/src/mcp/server/fastmcp/utilities/func_metadata.py#L105
def get_remote_tool_fn_metadata(tool: Tool) -> FuncMetadata:
    dynamic_pydantic_model_params: dict[str, Any] = {}
    for key in tool.inputSchema["properties"]:
        # Remote tools shouldn't have type annotations or default values
        # for their arguments. So, we set them to defaults.
        field_info = FieldInfo.from_annotated_attribute(
            annotation=_get_typed_annotation(
                annotation=Annotated[
                    Any,
                    Field(),
                    WithJsonSchema({"title": key, "type": "string"}),
                ],
                globalns={},
            ),
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


class ProxiedToolsManager:
    _stack = AsyncExitStack()

    async def get_remote_mcp_session(
        self, url: str, headers: dict[str, str]
    ) -> ClientSession:
        streamablehttp_client_context: tuple[
            MemoryObjectReceiveStream[SessionMessage | Exception],
            MemoryObjectSendStream[SessionMessage],
            GetSessionIdCallback,
        ] = await self._stack.enter_async_context(
            streamablehttp_client(
                url=url,
                headers=headers,
            )
        )
        read_stream, write_stream, _ = streamablehttp_client_context
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
    enabled_tools: set[ToolName],
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    """
    Register proxied MCP tools.

    Proxied tools are hosted remotely, so their definitions aren't found in this repo.
    """
    config = await config_provider.get_config()
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
                        f"Tool {tool_name} reported an error: "
                        + f"{tool_call_result.content}"
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
