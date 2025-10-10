import functools
import inspect
import logging
from collections.abc import Callable, Sequence
from typing import Any

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config import LspConfig
from dbt_mcp.lsp.lsp_binary_manager import dbt_lsp_binary_info
from dbt_mcp.lsp.lsp_client import LSPClient
from dbt_mcp.lsp.lsp_connection import LSPConnection
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.annotations import create_tool_annotations
from dbt_mcp.tools.definitions import ToolDefinition
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName

logger = logging.getLogger(__name__)

# Module-level LSP connection to manage lifecycle
_lsp_connection: LSPConnection | None = None


async def register_lsp_tools(
    server: FastMCP,
    config: LspConfig,
    exclude_tools: Sequence[ToolName] | None = None,
) -> None:
    register_tools(
        server,
        await list_lsp_tools(config),
        exclude_tools or [],
    )


async def list_lsp_tools(config: LspConfig) -> list[ToolDefinition]:
    """Register dbt Fusion tools with the MCP server.

    Args:
        config: LSP configuration containing LSP settings

    Returns:
        List of tool definitions for LSP tools
    """
    global _lsp_connection

    # Only initialize if not already initialized
    if _lsp_connection is None:
        lsp_binary_path = dbt_lsp_binary_info(config.lsp_path)

        if not lsp_binary_path:
            logger.warning("No LSP binary path found")
            return []

        logger.info(
            f"Using LSP binary in {lsp_binary_path.path} with version {lsp_binary_path.version}"
        )

        _lsp_connection = LSPConnection(
            binary_path=lsp_binary_path.path,
            args=[],
            cwd=config.project_dir,
        )

    def call_with_lsp_client(func: Callable) -> Callable:
        """Call a function with the LSP connection manager."""

        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            global _lsp_connection

            if _lsp_connection is None:
                return "LSP connection not initialized"

            if not _lsp_connection.state.initialized:
                try:
                    await _lsp_connection.start()
                    await _lsp_connection.initialize()
                    logger.info("LSP connection started and initialized successfully")

                except Exception as e:
                    logger.error(f"Error starting LSP connection: {e}")
                    # Clean up failed connection
                    _lsp_connection = None
                    return "Error: Failed to establish LSP connection"

            lsp_client = LSPClient(_lsp_connection)
            return await func(lsp_client, *args, **kwargs)

        # remove the lsp_client argument from the signature
        wrapper.__signature__ = inspect.signature(func).replace(  # type: ignore
            parameters=[
                param
                for param in inspect.signature(func).parameters.values()
                if param.name != "lsp_client"
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
    lsp_client: LSPClient,
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
        error_msg = (
            f"Failed to get column lineage for {model_id}.{column_name}: {str(e)}"
        )
        logger.error(error_msg)
        return {"error": error_msg}


async def cleanup_lsp_connection() -> None:
    """Clean up the LSP connection when shutting down."""
    global _lsp_connection
    if _lsp_connection:
        try:
            logger.info("Cleaning up LSP connection")
            await _lsp_connection.stop()
        except Exception as e:
            logger.error(f"Error cleaning up LSP connection: {e}")
        finally:
            _lsp_connection = None
