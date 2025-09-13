import json
import os
import subprocess
from collections.abc import Sequence

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
        args: dict[str, any] | None = None,
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
        args = {"schema_name": schema_name}
        if database_name:
            args["database_name"] = database_name
        if table_names:
            args["table_names"] = table_names
        if generate_columns:
            args["generate_columns"] = generate_columns
        if include_descriptions:
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
        args = {
            "model_names": model_names,
            "upstream_descriptions": upstream_descriptions,
            "include_data_types": include_data_types,
        }

        return _run_codegen_operation("generate_model_yaml", args)

    def generate_base_model(
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
        args = {
            "source_name": source_name,
            "table_name": table_name,
            "leading_commas": leading_commas,
            "case_sensitive_cols": case_sensitive_cols,
        }
        if materialized:
            args["materialized"] = materialized

        return _run_codegen_operation("generate_base_model", args)

    def generate_model_import_ctes(
        model_name: str = Field(description=get_prompt("dbt_codegen/args/model_name")),
        leading_commas: bool = Field(
            default=False, description=get_prompt("dbt_codegen/args/leading_commas")
        ),
    ) -> str:
        args = {
            "model_name": model_name,
            "leading_commas": leading_commas,
        }

        return _run_codegen_operation("generate_model_import_ctes", args)

    def create_base_models(
        source_name: str = Field(
            description=get_prompt("dbt_codegen/args/source_name")
        ),
        tables: list[str] = Field(description=get_prompt("dbt_codegen/args/tables")),
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
        """Generate creation instructions for multiple base models."""
        results = []

        for table in tables:
            # Generate base model for each table using existing function
            args = {
                "source_name": source_name,
                "table_name": table,
                "leading_commas": leading_commas,
                "case_sensitive_cols": case_sensitive_cols,
            }
            if materialized:
                args["materialized"] = materialized

            # Get the generated SQL
            sql_content = _run_codegen_operation("generate_base_model", args)

            # Check for errors
            if "Error:" in sql_content:
                return sql_content  # Return first error encountered

            # Format the result
            filename = f"stg_{source_name}__{table}.sql"
            results.append(
                f"File: {filename}\n{'-' * (len(filename) + 6)}\n{sql_content}"
            )

        return "\n\n".join(results)

    def base_model_creation(
        source_name: str = Field(
            description=get_prompt("dbt_codegen/args/source_name")
        ),
        tables: list[str] = Field(description=get_prompt("dbt_codegen/args/tables")),
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
        """Create actual model files for multiple base models."""
        # Determine models directory path
        models_dir = os.path.join(config.project_dir, "models")

        # Check if models directory exists, create if it doesn't
        try:
            os.makedirs(models_dir, exist_ok=True)
        except Exception as e:
            return f"Error: Cannot access or create models directory: {e}"

        created_files = []

        for table in tables:
            # Generate base model for each table
            args = {
                "source_name": source_name,
                "table_name": table,
                "leading_commas": leading_commas,
                "case_sensitive_cols": case_sensitive_cols,
            }
            if materialized:
                args["materialized"] = materialized

            # Get the generated SQL
            sql_content = _run_codegen_operation("generate_base_model", args)

            # Check for errors
            if "Error:" in sql_content:
                return sql_content  # Return first error encountered

            # Create filename following dbt convention
            filename = f"stg_{source_name}__{table}.sql"
            filepath = os.path.join(models_dir, filename)

            try:
                # Write the file
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(sql_content)

                created_files.append(filepath)

            except Exception as e:
                return f"Error writing file {filename}: {e}"

        # Return success message
        files_list = "\n".join([f"  - {f}" for f in created_files])
        return (
            f"Successfully created {len(created_files)} base model files:\n{files_list}"
        )

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
            fn=generate_base_model,
            description=get_prompt("dbt_codegen/generate_base_model"),
            annotations=create_tool_annotations(
                title="dbt-codegen generate_base_model",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            fn=generate_model_import_ctes,
            description=get_prompt("dbt_codegen/generate_model_import_ctes"),
            annotations=create_tool_annotations(
                title="dbt-codegen generate_model_import_ctes",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            fn=create_base_models,
            description=get_prompt("dbt_codegen/create_base_models"),
            annotations=create_tool_annotations(
                title="dbt-codegen create_base_models",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            fn=base_model_creation,
            description=get_prompt("dbt_codegen/base_model_creation"),
            annotations=create_tool_annotations(
                title="dbt-codegen base_model_creation",
                read_only_hint=False,
                destructive_hint=True,
                idempotent_hint=False,
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
