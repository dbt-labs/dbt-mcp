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
    enabled_tools: set[ToolName] | None,
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

    def make_definition(
        func: Callable,
        tool_name: ToolName,
        title: str,
        prompt: str,
        *,
        read_only: bool = True,
    ) -> ToolDefinition:
        return ToolDefinition(
            fn=call_with_lsp_client(func),
            name=tool_name.value,
            title=title,
            description=get_prompt(prompt),
            annotations=create_tool_annotations(
                read_only_hint=read_only,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        )

    return [
        make_definition(
            get_column_lineage,
            ToolName.GET_COLUMN_LINEAGE,
            "Get Column Lineage",
            "lsp/get_column_lineage",
            read_only=False,
        ),
        make_definition(
            dbt_lsp_project_status,
            ToolName.DBT_LSP_PROJECT_STATUS,
            "Get dbt LSP Project Status",
            "lsp/project_status",
        ),
        make_definition(
            dbt_lsp_diagnostics,
            ToolName.DBT_LSP_DIAGNOSTICS,
            "Get dbt Diagnostics",
            "lsp/diagnostics",
        ),
        make_definition(
            dbt_lsp_node,
            ToolName.DBT_LSP_NODE,
            "Get dbt Node",
            "lsp/node",
        ),
        make_definition(
            dbt_lsp_lineage,
            ToolName.DBT_LSP_LINEAGE,
            "Get dbt Lineage",
            "lsp/lineage",
        ),
        make_definition(
            dbt_lsp_definition,
            ToolName.DBT_LSP_DEFINITION,
            "Resolve dbt Definition",
            "lsp/definition",
        ),
        make_definition(
            dbt_lsp_references,
            ToolName.DBT_LSP_REFERENCES,
            "Find dbt References",
            "lsp/references",
        ),
        make_definition(
            dbt_lsp_compile,
            ToolName.DBT_LSP_COMPILE,
            "Compile dbt Project with LSP",
            "lsp/compile",
            read_only=False,
        ),
        make_definition(
            dbt_lsp_preview,
            ToolName.DBT_LSP_PREVIEW,
            "Preview Compiled dbt SQL",
            "lsp/preview",
        ),
        make_definition(
            dbt_lsp_code_actions,
            ToolName.DBT_LSP_CODE_ACTIONS,
            "Preview dbt Code Actions",
            "lsp/code_actions",
        ),
        make_definition(
            dbt_lsp_rename_preview,
            ToolName.DBT_LSP_RENAME_PREVIEW,
            "Preview dbt Rename",
            "lsp/rename_preview",
        ),
        make_definition(
            dbt_lsp_update_document,
            ToolName.DBT_LSP_UPDATE_DOCUMENT,
            "Update dbt Document Incrementally",
            "lsp/update_document",
            read_only=False,
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


async def _call_lsp(
    lsp_client_provider: LSPClientProvider,
    operation: str,
    label: str,
    **kwargs: Any,
) -> dict[str, Any]:
    try:
        client = await lsp_client_provider.get_client()
        return await getattr(client, operation)(**kwargs)
    except TimeoutError:
        return {"error": f"Timeout waiting for {label}"}
    except Exception as exc:
        logger.exception("LSP operation failed: %s", operation)
        return {"error": f"Failed to {label}: {exc!s}"}


async def dbt_lsp_project_status(
    lsp_client_provider: LSPClientProvider,
) -> dict[str, Any]:
    return await _call_lsp(
        lsp_client_provider, "get_project_info", "get project status"
    )


async def dbt_lsp_diagnostics(
    lsp_client_provider: LSPClientProvider,
    path: str = Field(description=get_prompt("lsp/args/path")),
) -> dict[str, Any]:
    return await _call_lsp(
        lsp_client_provider, "get_diagnostics", "get diagnostics", path=path
    )


async def dbt_lsp_node(
    lsp_client_provider: LSPClientProvider,
    path: str = Field(description=get_prompt("lsp/args/path")),
) -> dict[str, Any]:
    return await _call_lsp(lsp_client_provider, "get_node", "get node", path=path)


async def dbt_lsp_lineage(
    lsp_client_provider: LSPClientProvider,
    model_selector: str = Field(description=get_prompt("lsp/args/model_selector")),
) -> dict[str, Any]:
    return await _call_lsp(
        lsp_client_provider,
        "get_model_lineage",
        "get lineage",
        model_selector=model_selector,
    )


async def dbt_lsp_definition(
    lsp_client_provider: LSPClientProvider,
    path: str = Field(description=get_prompt("lsp/args/path")),
    line: int = Field(description=get_prompt("lsp/args/line")),
    character: int = Field(description=get_prompt("lsp/args/character")),
) -> dict[str, Any]:
    return await _call_lsp(
        lsp_client_provider,
        "get_definition",
        "resolve definition",
        path=path,
        line=line,
        character=character,
    )


async def dbt_lsp_references(
    lsp_client_provider: LSPClientProvider,
    path: str = Field(description=get_prompt("lsp/args/path")),
    line: int = Field(description=get_prompt("lsp/args/line")),
    character: int = Field(description=get_prompt("lsp/args/character")),
    include_declaration: bool = Field(
        default=True, description=get_prompt("lsp/args/include_declaration")
    ),
) -> dict[str, Any]:
    return await _call_lsp(
        lsp_client_provider,
        "get_references",
        "find references",
        path=path,
        line=line,
        character=character,
        include_declaration=include_declaration,
    )


async def dbt_lsp_compile(
    lsp_client_provider: LSPClientProvider,
) -> dict[str, Any]:
    return await _call_lsp(lsp_client_provider, "compile", "compile the dbt project")


async def dbt_lsp_preview(
    lsp_client_provider: LSPClientProvider,
    path: str = Field(description=get_prompt("lsp/args/path")),
) -> dict[str, Any]:
    return await _call_lsp(
        lsp_client_provider, "compile_file", "preview compiled SQL", path=path
    )


async def dbt_lsp_code_actions(
    lsp_client_provider: LSPClientProvider,
    path: str = Field(description=get_prompt("lsp/args/path")),
    start_line: int = Field(description=get_prompt("lsp/args/line")),
    start_character: int = Field(description=get_prompt("lsp/args/character")),
    end_line: int = Field(description=get_prompt("lsp/args/end_line")),
    end_character: int = Field(description=get_prompt("lsp/args/end_character")),
    only: list[str] | None = Field(
        default=None, description=get_prompt("lsp/args/code_action_kinds")
    ),
) -> dict[str, Any]:
    return await _call_lsp(
        lsp_client_provider,
        "get_code_actions",
        "get code actions",
        path=path,
        start_line=start_line,
        start_character=start_character,
        end_line=end_line,
        end_character=end_character,
        only=only,
    )


async def dbt_lsp_rename_preview(
    lsp_client_provider: LSPClientProvider,
    path: str = Field(description=get_prompt("lsp/args/path")),
    line: int = Field(description=get_prompt("lsp/args/line")),
    character: int = Field(description=get_prompt("lsp/args/character")),
    new_name: str = Field(description=get_prompt("lsp/args/new_name")),
) -> dict[str, Any]:
    return await _call_lsp(
        lsp_client_provider,
        "get_rename_preview",
        "preview rename",
        path=path,
        line=line,
        character=character,
        new_name=new_name,
    )


async def dbt_lsp_update_document(
    lsp_client_provider: LSPClientProvider,
    path: str = Field(description=get_prompt("lsp/args/path")),
    text: str = Field(description=get_prompt("lsp/args/text")),
    wait_for_compile: bool = Field(
        default=True, description=get_prompt("lsp/args/wait_for_compile")
    ),
) -> dict[str, Any]:
    return await _call_lsp(
        lsp_client_provider,
        "update_document",
        "update the document",
        path=path,
        text=text,
        wait_for_compile=wait_for_compile,
    )
