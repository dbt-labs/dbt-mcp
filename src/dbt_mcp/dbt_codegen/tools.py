import json
import os
import subprocess
from collections.abc import Sequence
from typing import Any

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_mcp.config.config import DbtCodegenConfig
from dbt_mcp.dbt_cli.binary_type import get_color_disable_flag
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.definitions import ToolDefinition
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.annotations import create_tool_annotations


def create_dbt_codegen_tool_definitions(
    config: DbtCodegenConfig,
) -> list[ToolDefinition]:
    def _run_codegen_operation(
        macro_name: str,
        args: dict[str, Any] | None = None,
    ) -> str:
        """Execute a dbt-codegen macro using dbt run-operation."""
        try:
            # Build the dbt run-operation command
            command = ["run-operation", macro_name]

            # Add arguments if provided
            if args:
                # Convert args to JSON string for dbt
                args_json = json.dumps(args)
                command.extend(["--args", args_json])

            full_command = command.copy()
            # Add --quiet flag to reduce output verbosity
            main_command = full_command[0]
            command_args = full_command[1:] if len(full_command) > 1 else []
            full_command = [main_command, "--quiet", *command_args]

            # We change the path only if this is an absolute path, otherwise we can have
            # problems with relative paths applied multiple times as DBT_PROJECT_DIR
            # is applied to dbt Core and Fusion as well (but not the dbt Cloud CLI)
            cwd_path = config.project_dir if os.path.isabs(config.project_dir) else None

            # Add appropriate color disable flag based on binary type
            color_flag = get_color_disable_flag(config.binary_type)
            args_list = [config.dbt_path, color_flag, *full_command]

            process = subprocess.Popen(
                args=args_list,
                cwd=cwd_path,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                text=True,
            )
            output, _ = process.communicate(timeout=config.dbt_cli_timeout)

            # Return the output directly or handle errors
            if process.returncode != 0:
                if "dbt found" in output and "resource" in output:
                    return f"Error: dbt-codegen package may not be installed. Run 'dbt deps' to install it.\n{output}"
                return f"Error running dbt-codegen macro: {output}"

            return output or "OK"

        except subprocess.TimeoutExpired:
            return f"Timeout: dbt-codegen operation took longer than {config.dbt_cli_timeout} seconds."
        except Exception as e:
            return str(e)

    def generate_source(
        schema_name: str = Field(
            description=get_prompt("dbt_codegen/args/schema_name")
        ),
        database_name: str | None = Field(
            default=None, description=get_prompt("dbt_codegen/args/database_name")
        ),
        table_names: list[str] | None = Field(
            default=None, description=get_prompt("dbt_codegen/args/table_names")
        ),
        generate_columns: bool = Field(
            default=False, description=get_prompt("dbt_codegen/args/generate_columns")
        ),
        include_descriptions: bool = Field(
            default=False,
            description=get_prompt("dbt_codegen/args/include_descriptions"),
        ),
    ) -> str:
        args: dict[str, Any] = {"schema_name": schema_name}
        if database_name:
            args["database_name"] = database_name
        if table_names:
            args["table_names"] = table_names
        args["generate_columns"] = generate_columns
        args["include_descriptions"] = include_descriptions

        return _run_codegen_operation("generate_source", args)

    def generate_model_yaml(
        model_names: list[str] = Field(
            description=get_prompt("dbt_codegen/args/model_names")
        ),
        upstream_descriptions: bool = Field(
            default=False,
            description=get_prompt("dbt_codegen/args/upstream_descriptions"),
        ),
        include_data_types: bool = Field(
            default=True, description=get_prompt("dbt_codegen/args/include_data_types")
        ),
    ) -> str:
        args: dict[str, Any] = {
            "model_names": model_names,
            "upstream_descriptions": upstream_descriptions,
            "include_data_types": include_data_types,
        }

        return _run_codegen_operation("generate_model_yaml", args)

    def generate_staging_model(
        source_name: str = Field(
            description=get_prompt("dbt_codegen/args/source_name")
        ),
        table_name: str = Field(description=get_prompt("dbt_codegen/args/table_name")),
        leading_commas: bool = Field(
            default=False, description=get_prompt("dbt_codegen/args/leading_commas")
        ),
        case_sensitive_cols: bool = Field(
            default=False,
            description=get_prompt("dbt_codegen/args/case_sensitive_cols"),
        ),
        materialized: str | None = Field(
            default=None, description=get_prompt("dbt_codegen/args/materialized")
        ),
    ) -> str:
        args: dict[str, Any] = {
            "source_name": source_name,
            "table_name": table_name,
            "leading_commas": leading_commas,
            "case_sensitive_cols": case_sensitive_cols,
        }
        if materialized:
            args["materialized"] = materialized

        return _run_codegen_operation("generate_base_model", args)

    return [
        ToolDefinition(
            fn=generate_source,
            description=get_prompt("dbt_codegen/generate_source"),
            annotations=create_tool_annotations(
                title="dbt-codegen generate_source",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            fn=generate_model_yaml,
            description=get_prompt("dbt_codegen/generate_model_yaml"),
            annotations=create_tool_annotations(
                title="dbt-codegen generate_model_yaml",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            fn=generate_staging_model,
            description=get_prompt("dbt_codegen/generate_staging_model"),
            annotations=create_tool_annotations(
                title="dbt-codegen generate_staging_model",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
    ]


def register_dbt_codegen_tools(
    dbt_mcp: FastMCP,
    config: DbtCodegenConfig,
    exclude_tools: Sequence[ToolName] = [],
) -> None:
    register_tools(
        dbt_mcp,
        create_dbt_codegen_tool_definitions(config),
        exclude_tools,
    )
