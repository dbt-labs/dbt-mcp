"""Module for discovering and parsing custom tool models from dbt project."""

import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jinja2 import nodes
from jinja2.sandbox import ImmutableSandboxedEnvironment
from pydantic import BaseModel, ValidationError

from dbt_mcp.custom_tools.filesystem import (
    FileSystemProvider,
    LocalFileSystemProvider,
)

logger = logging.getLogger(__name__)


@dataclass
class ModelVariable:
    """Represents a Jinja variable in a model."""

    name: str
    default_value: str | None = None
    is_required: bool = True


@dataclass
class CustomToolModel:
    """Represents a custom tool model discovered from dbt project."""

    name: str
    file_path: Path
    sql_template: str
    variables: list[ModelVariable]
    description: str | None = None


class DbtModelInfo(BaseModel):
    """Pydantic model for parsing dbt ls JSON output."""

    name: str
    original_file_path: str
    description: str | None = None


class JinjaTemplateParser:
    """Handles parsing and rendering of Jinja2 templates"""

    def __init__(self) -> None:
        self._env = ImmutableSandboxedEnvironment()

    def extract_jinja_variables(self, sql_template: str) -> list[ModelVariable]:
        """
        Extract Jinja variables from SQL template using AST parsing.

        Looks for patterns like:
        - {{ var('variable_name') }}
        - {{ var('variable_name', 'default_value') }}

        Args:
            sql_template: The SQL template string with Jinja syntax

        Returns:
            List of ModelVariable objects
        """
        variables: dict[str, ModelVariable] = {}

        try:
            ast = self._env.parse(sql_template)

            # Walk through the AST to find var() calls
            for node in ast.find_all(nodes.Call):
                # Check if this is a call to 'var' with at least one argument
                if (
                    isinstance(node.node, nodes.Name)
                    and node.node.name == "var"
                    and node.args
                    and isinstance(node.args[0], nodes.Const)
                ):
                    # Extract variable name (first argument)
                    var_name = node.args[0].value

                    # Extract default value (second argument, if present)
                    default_value = None
                    if len(node.args) > 1 and isinstance(node.args[1], nodes.Const):
                        default_value = node.args[1].value

                    if var_name not in variables:
                        variables[var_name] = ModelVariable(
                            name=var_name,
                            default_value=default_value,
                            is_required=default_value is None,
                        )

            logger.debug(f"Extracted variables via AST: {list(variables.keys())}")
            return list(variables.values())

        except Exception as e:
            logger.warning(f"Failed to parse template with Jinja2 AST: {e}")
            return []

    def render_model_sql(
        self, model: CustomToolModel, parameters: dict[str, Any]
    ) -> str:
        """
        Render the SQL template with provided parameters.

        Args:
            model: The CustomToolModel to render
            parameters: Dictionary of parameter values

        Returns:
            Rendered SQL string

        Raises:
            ValueError: If required variables are missing
        """
        # Check for required variables
        missing_vars = [
            var.name
            for var in model.variables
            if var.is_required and var.name not in parameters
        ]

        if missing_vars:
            raise ValueError(
                f"Missing required variables for model '{model.name}': {missing_vars}"
            )

        # Create helper functions for dbt Jinja functions
        def var(name: str, default: str | None = None) -> str:
            """Handle var() function calls."""
            value = parameters.get(name, default if default is not None else "")
            return str(value)

        def ref(model_name: str) -> str:
            """Handle ref() function calls - returns a placeholder."""
            return f"<ref:{model_name}>"

        def source(source_name: str, table_name: str) -> str:
            """Handle source() function calls - returns a placeholder."""
            return f"<source:{source_name}.{table_name}>"

        def config(**kwargs: Any) -> str:
            """Handle config() function calls - returns empty string."""
            return ""

        # Render the template
        try:
            template = self._env.from_string(model.sql_template)

            # Add dbt functions to the template context
            rendered_sql = template.render(
                var=var, ref=ref, source=source, config=config, **parameters
            )
            return rendered_sql
        except Exception as e:
            raise ValueError(
                f"Failed to render template for model '{model.name}': {e}"
            ) from e


def discover_tool_models(
    project_dir: str,
    tools_subdir: str,
    dbt_path: str,
    parser: JinjaTemplateParser,
    fs_provider: FileSystemProvider | None = None,
) -> list[CustomToolModel]:
    """
    Discover custom tool models in the dbt project.

    Args:
        project_dir: Path to the dbt project root directory
        tools_subdir: Subdirectory containing tool models
        dbt_path: Path to the dbt executable
        parser: JinjaTemplateParser instance for extracting variables
        fs_provider: File system provider for reading files
            (defaults to LocalFileSystemProvider)

    Returns:
        List of CustomToolModel objects
    """
    # Use local file system provider by default
    if fs_provider is None:
        fs_provider = LocalFileSystemProvider()

    models: list[CustomToolModel] = []
    try:
        # Run dbt ls to get all models in the tools directory
        # --output json gives us structured data
        # --output-keys specifies what fields to include
        command = [
            dbt_path,
            "ls",
            "--select",
            f"path:{tools_subdir}",
            "--resource-type",
            "model",
            "--output",
            "json",
            "--output-keys",
            "name",
            "original_file_path",
            "description",
        ]

        logger.debug(f"Running dbt ls command: {' '.join(command)}")

        result = subprocess.run(
            command,
            cwd=project_dir,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

        if result.returncode != 0:
            logger.error(f"dbt ls command failed: {result.stderr}")
            return []

        # Parse JSON output - each line is a separate JSON object
        for line in result.stdout.strip().split("\n"):
            if not line:
                continue

            try:
                model_info = DbtModelInfo.model_validate_json(line)

                # Resolve the full path to the SQL file using fs_provider
                sql_file_path = fs_provider.join_path(
                    project_dir, model_info.original_file_path
                )

                if not fs_provider.exists(sql_file_path):
                    logger.warning(f"Model file does not exist: {sql_file_path}")
                    continue

                # Read the SQL template using fs_provider
                sql_template = fs_provider.read_text(sql_file_path)

                # Extract Jinja variables
                variables = parser.extract_jinja_variables(sql_template)

                # Create model object
                # (keep Path for file_path for backward compatibility)
                model = CustomToolModel(
                    name=model_info.name,
                    file_path=Path(sql_file_path),
                    sql_template=sql_template,
                    variables=variables,
                    description=model_info.description
                    or f"Execute SQL for the {model_info.name} model",
                )
                models.append(model)
            except ValidationError as e:
                logger.error(f"Failed to parse JSON line: {line} - {e}")
                continue
            except Exception as e:
                logger.error(f"Failed to process model: {e}")
                continue

    except subprocess.TimeoutExpired:
        logger.error("dbt ls command timed out")
        return []
    except Exception as e:
        logger.error(f"Failed to run dbt ls: {e}")
        return []

    return models
