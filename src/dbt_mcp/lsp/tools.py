import functools
import inspect
import logging
from collections.abc import Callable
from typing import Any

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.lsp.providers.lsp_client_provider import LSPClientProvider
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.annotations import create_tool_annotations
from dbt_mcp.tools.definitions import ToolDefinition
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset

logger = logging.getLogger(__name__)


async def register_lsp_tools(
    server: FastMCP,
    lspClientProvider: LSPClientProvider,
    *,
    disabled_tools: set[ToolName],
    enabled_tools: set[ToolName],
    enabled_toolsets: set[Toolset],
    disabled_toolsets: set[Toolset],
) -> None:
    register_tools(
        dbt_mcp=server,
        tool_definitions=await list_lsp_tools(lspClientProvider),
        disabled_tools=disabled_tools,
        enabled_tools=enabled_tools,
        enabled_toolsets=enabled_toolsets,
        disabled_toolsets=disabled_toolsets,
    )


async def list_lsp_tools(
    lspClientProvider: LSPClientProvider,
) -> list[ToolDefinition]:
    """Register dbt Fusion tools with the MCP server.

    Args:
        config: LSP configuration containing LSP settings

    Returns:
        List of tool definitions for LSP tools
    """

    def call_with_lsp_client(func: Callable) -> Callable:
        """Call a function with the LSP connection manager."""

        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            return await func(lspClientProvider, *args, **kwargs)

        # remove the lsp_client argument from the signature
        wrapper.__signature__ = inspect.signature(func).replace(  # type: ignore
            parameters=[
                param
                for param in inspect.signature(func).parameters.values()
                if param.name != "lsp_client_provider"
            ]
        )

        return wrapper

    return [
        ToolDefinition(
            fn=call_with_lsp_client(get_column_lineage),
            description=get_prompt("lsp/get_column_lineage"),
            annotations=create_tool_annotations(
                title="get_column_lineage",
                read_only_hint=False,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
    ]


async def get_column_lineage(
    lsp_client_provider: LSPClientProvider,
    model_id: str = Field(description=get_prompt("lsp/args/model_id")),
    column_name: str = Field(description=get_prompt("lsp/args/column_name")),
) -> dict[str, Any]:
    """Get column lineage for a specific model column.

    Args:
        lsp_client: The LSP client instance
        model_id: The dbt model identifier
        column_name: The column name to trace lineage for

    Returns:
        Dictionary with either:
        - 'nodes' key containing lineage information on success
        - 'error' key containing error message on failure
    """
    try:
        lsp_client = await lsp_client_provider.get_client()
        response = await lsp_client.get_column_lineage(
            model_id=model_id,
            column_name=column_name,
        )

        # Check for LSP-level errors
        if "error" in response:
            logger.error(f"LSP error getting column lineage: {response['error']}")
            return {"error": f"LSP error: {response['error']}"}

        # Validate response has expected data
        if "nodes" not in response or not response["nodes"]:
            logger.warning(f"No column lineage found for {model_id}.{column_name}")
            return {
                "error": f"No column lineage found for model {model_id} and column {column_name}"
            }

        return {"nodes": response["nodes"]}

    except TimeoutError:
        error_msg = f"Timeout waiting for column lineage (model: {model_id}, column: {column_name})"
        logger.error(error_msg)
        return {"error": error_msg}
    except Exception as e:
        error_msg = f"Failed to get column lineage for {model_id}.{column_name}: {e!s}"
        logger.error(error_msg)
        return {"error": error_msg}
