import json
import logging
import os
import subprocess
from collections.abc import Sequence
from typing import (
    Any,
)

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.tools.base import Tool as InternalTool
from mcp.server.fastmcp.utilities.func_metadata import (
    ArgModelBase,
    FuncMetadata,
)
from pydantic import create_model
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from dbt_mcp.config.config import CustomToolsConfig
from dbt_mcp.custom_tools.filesystem import FileSystemProvider
from dbt_mcp.custom_tools.model_discovery import (
    CustomToolModel,
    JinjaTemplateParser,
    ModelVariable,
    discover_tool_models,
)
from dbt_mcp.dbt_cli.binary_type import get_color_disable_flag
from dbt_mcp.tools.annotations import create_tool_annotations
from dbt_mcp.tools.tool_names import ToolName

logger = logging.getLogger(__name__)


def convert_variables_to_json_schema(
    variables: list[ModelVariable],
) -> dict[str, Any]:
    """
    Convert model variables to JSON schema format.

    Args:
        variables: List of ModelVariable objects with name, default_value, is_required

    Returns:
        JSON schema dict with type, properties, and required fields
    """
    if not variables:
        return {"type": "object", "properties": {}}

    properties = {}
    required = []

    for var in variables:
        # dbt vars are passed as JSON, so they can be any valid JSON type
        # Use a union of all JSON types for explicitness
        prop_schema: dict[str, Any] = {
            "type": ["string", "number", "boolean", "null", "object", "array"]
        }

        # Add default value using JSON Schema's default keyword
        if var.default_value is not None:
            prop_schema["default"] = var.default_value

        properties[var.name] = prop_schema

        # Add to required list if no default value
        if var.default_value is None:
            required.append(var.name)

    schema = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required

    return schema


# Based on this: https://github.com/modelcontextprotocol/python-sdk/blob/9ae4df85fbab97bf476ddd160b766ca4c208cd13/src/mcp/server/fastmcp/utilities/func_metadata.py#L105
def get_custom_tool_fn_metadata(custom_tool_model: CustomToolModel) -> FuncMetadata:
    dynamic_pydantic_model_params: dict[str, Any] = {}
    for variable in custom_tool_model.variables:
        # Remote tools shouldn't have type annotations or default values
        # for their arguments. So, we set them to defaults.
        field_info = FieldInfo.from_annotated_attribute(
            annotation=Any,
            default=PydanticUndefined,
        )
        dynamic_pydantic_model_params[variable.name] = (field_info.annotation, None)
    return FuncMetadata(
        arg_model=create_model(
            f"{custom_tool_model.name}Arguments",
            **dynamic_pydantic_model_params,
            __base__=ArgModelBase,
        )
    )


def register_custom_tools(
    dbt_mcp: FastMCP,
    config_provider: CustomToolsConfig,
    exclude_tools: Sequence[ToolName],
    fs_provider: FileSystemProvider,
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

    for model in models:
        # Create a function for this tool that accepts the
        # model's variables as parameters
        def create_tool_function(m: CustomToolModel):
            async def tool_function(**kwargs) -> str:
                """Execute custom tool SQL with provided parameters."""
                # Execute SQL using dbt show - let dbt handle Jinja rendering
                try:
                    # Convert kwargs to JSON string for --vars parameter
                    vars_json = json.dumps(kwargs) if kwargs else None

                    # Build the dbt show command
                    cwd_path = (
                        config_provider.project_dir
                        if os.path.isabs(config_provider.project_dir)
                        else None
                    )

                    # Add appropriate color disable flag based on binary type
                    color_flag = get_color_disable_flag(config_provider.binary_type)
                    args = [
                        config_provider.dbt_path,
                        color_flag,
                        "show",
                        "--inline",
                        m.sql_template,
                        "--favor-state",  # TODO: need this?
                        "--limit",
                        "-1",  # negative limit means no limit
                        "--output",
                        "json",
                    ]

                    # Add vars if provided - dbt will use these for rendering
                    if vars_json:
                        args.extend(["--vars", vars_json])

                    # Execute the command
                    process = subprocess.Popen(
                        args=args,
                        cwd=cwd_path,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        stdin=subprocess.DEVNULL,
                        text=True,
                    )
                    output, _ = process.communicate(
                        timeout=config_provider.dbt_cli_timeout
                    )
                    return output or "OK"
                except subprocess.TimeoutExpired:
                    return (
                        f"Timeout: dbt show command for {m.name} "
                        "took too long to complete."
                    )
                except Exception as e:
                    logger.error(f"Failed to execute {m.name}: {e}")
                    raise e

            # Dynamically set the function signature based on model variables
            # Add annotations for each parameter
            # for var in m.variables:
            #     tool_function.__annotations__[var.name] = str

            return tool_function

        if model.name.lower() in [tool.value.lower() for tool in exclude_tools]:
            continue
        title = model.name.replace("_", " ").title()
        dbt_mcp._tool_manager._tools[model.name] = InternalTool(
            fn=create_tool_function(model),
            title=title,
            name=model.name,
            annotations=create_tool_annotations(
                title=title,
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
            description=model.description or f"Execute custom tool: {model.name}",
            parameters=convert_variables_to_json_schema(model.variables),
            fn_metadata=get_custom_tool_fn_metadata(model),
            is_async=True,
            context_kwarg=None,
        )
