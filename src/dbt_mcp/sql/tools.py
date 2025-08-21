import logging
from collections.abc import Sequence
from contextlib import AsyncExitStack
from typing import (
    Annotated,
    Any,
)

from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from mcp import ClientSession
from mcp.client.streamable_http import GetSessionIdCallback, streamablehttp_client
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.tools.base import Tool as InternalTool
from mcp.server.fastmcp.utilities.func_metadata import (
    ArgModelBase,
    FuncMetadata,
    _get_typed_annotation,
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

from dbt_mcp.config.config import SqlConfig
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset, toolsets

logger = logging.getLogger(__name__)


# Based on this: https://github.com/modelcontextprotocol/python-sdk/blob/9ae4df85fbab97bf476ddd160b766ca4c208cd13/src/mcp/server/fastmcp/utilities/func_metadata.py#L105
def get_remote_tool_fn_metadata(tool: Tool) -> FuncMetadata:
    dynamic_pydantic_model_params: dict[str, Any] = {}
    for key in tool.inputSchema["properties"].keys():
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


async def _get_sql_tools(session: ClientSession) -> list[Tool]:
    try:
        sql_tool_names = {t.value for t in toolsets[Toolset.SQL]}
        return [
            t for t in (await session.list_tools()).tools if t.name in sql_tool_names
        ]
    except Exception as e:
        logger.error(f"Error getting SQL tools: {e}")
        return []


class SqlToolsManager:
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


async def register_sql_tools(
    dbt_mcp: FastMCP,
    config: SqlConfig,
    exclude_tools: Sequence[ToolName] = [],
) -> None:
    """
    Register SQL MCP tools.

    SQL tools are hosted remotely, so their definitions aren't found in this repo.
    """

    is_local = config.host and config.host.startswith("localhost")
    path = "/v1/mcp/" if is_local else "/api/ai/v1/mcp/"
    scheme = "http://" if is_local else "https://"
    multicell_account_prefix = (
        f"{config.multicell_account_prefix}." if config.multicell_account_prefix else ""
    )
    url = f"{scheme}{multicell_account_prefix}{config.host}{path}"
    headers = {
        "Authorization": f"Bearer {config.token}",
        "x-dbt-prod-environment-id": str(config.prod_environment_id),
        "x-dbt-dev-environment-id": str(config.dev_environment_id),
        "x-dbt-user-id": str(config.user_id),
    }
    sql_tools_manager = SqlToolsManager()
    session = await sql_tools_manager.get_remote_mcp_session(url, headers)
    await session.initialize()
    sql_tools = await _get_sql_tools(session)
    logger.info(f"Loaded sql tools: {', '.join([tool.name for tool in sql_tools])}")
    for tool in sql_tools:
        if tool.name.lower() in [tool.value.lower() for tool in exclude_tools]:
            continue

        # Create a new function using a factory to avoid closure issues
        def create_tool_function(tool_name: str):
            async def tool_function(*args, **kwargs) -> Sequence[ContentBlock]:
                try:
                    tool_call_result = await session.call_tool(
                        tool_name,
                        kwargs,
                    )
                    if tool_call_result.isError:
                        raise ValueError(
                            f"Tool {tool_name} reported an error: "
                            + f"{tool_call_result.content}"
                        )
                    return tool_call_result.content
                except Exception as e:
                    return [
                        TextContent(
                            type="text",
                            text=str(e),
                        )
                    ]

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
