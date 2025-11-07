import logging
from collections.abc import Sequence

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import CustomToolsConfig
from dbt_mcp.custom_tools.filesystem import FileSystemProvider
from dbt_mcp.custom_tools.model_discovery import (
    JinjaTemplateParser,
    discover_tool_models,
)
from dbt_mcp.tools.annotations import create_tool_annotations
from dbt_mcp.tools.definitions import ToolDefinition
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName

logger = logging.getLogger(__name__)


def create_custom_tool_definitions(
    config_provider: CustomToolsConfig,
    fs_provider: FileSystemProvider | None = None,
) -> list[ToolDefinition]:
    """
    Create tool definitions from custom tool models in the dbt project.

    Steps:
    1. Read models in /tools directory
    2. Parse Jinja template vars from each model
    3. Create tool definitions for each model
       - description: model description
       - name: model name
       - body: execute SQL with provided parameters

    Args:
        config_provider: Configuration for custom tools
        fs_provider: File system provider for reading files
            (defaults to LocalFileSystemProvider)

    Returns:
        List of ToolDefinition objects
    """
    # Create parser with sandboxed Jinja environment
    parser = JinjaTemplateParser()

    # Discover models from the tools directory using dbt ls
    models = discover_tool_models(
        config_provider.project_dir,
        "models/tools",
        config_provider.dbt_path,
        parser,
        fs_provider,
    )

    print(models)

    if not models:
        logger.info("No custom tool models found in models/tools directory")
        return []

    logger.info(f"Discovered {len(models)} custom tool models")

    tool_definitions: list[ToolDefinition] = []

    for model in models:
        # Create a function for this tool that accepts the
        # model's variables as parameters
        def create_tool_function(model_instance, template_parser):
            async def tool_function(**kwargs) -> str:
                """Execute custom tool SQL with provided parameters."""
                try:
                    # Render the SQL with parameters
                    rendered_sql = template_parser.render_model_sql(
                        model_instance, kwargs
                    )

                    # TODO: Execute SQL (integrate with SQL tools or CLI)
                    # For now, just return the rendered SQL
                    result = (
                        f"Rendered SQL for {model_instance.name}:\n\n{rendered_sql}"
                    )
                    return result

                except Exception as e:
                    return f"Error executing custom tool '{model_instance.name}': {e!s}"

            # Dynamically set the function signature based on model variables
            # Add annotations for each parameter
            for var in model_instance.variables:
                tool_function.__annotations__[var.name] = str

            return tool_function

        # Create the tool definition
        tool_def = ToolDefinition(
            name=model.name,
            description=(model.description or f"Execute custom tool: {model.name}"),
            fn=create_tool_function(model, parser),
            annotations=create_tool_annotations(
                title=model.name.replace("_", " ").title(),
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        )

        tool_definitions.append(tool_def)
        logger.info(
            f"Created tool definition for '{model.name}' with "
            f"{len(model.variables)} parameters"
        )

    return tool_definitions


def register_custom_tools(
    dbt_mcp: FastMCP,
    config_provider: CustomToolsConfig,
    exclude_tools: Sequence[ToolName],
    fs_provider: FileSystemProvider | None = None,
) -> None:
    """
    Register custom tools from models/tools directory.

    Args:
        dbt_mcp: FastMCP instance to register tools with
        config_provider: Configuration for custom tools
        exclude_tools: List of tool names to exclude from registration
        fs_provider: File system provider for reading files
            (defaults to LocalFileSystemProvider)
    """
    register_tools(
        dbt_mcp,
        create_custom_tool_definitions(config_provider, fs_provider),
        exclude_tools,
    )
