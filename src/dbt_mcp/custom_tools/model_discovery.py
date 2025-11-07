"""Module for discovering and parsing custom tool models from dbt project."""

import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jinja2 import ChainableUndefined
from jinja2.sandbox import ImmutableSandboxedEnvironment
from pydantic import BaseModel, ValidationError

from dbt_mcp.custom_tools.filesystem import (
    FileSystemProvider,
)

logger = logging.getLogger(__name__)


@dataclass
class ModelVariable:
    """Represents a Jinja variable in a model."""

    name: str
    default_value: Any | None = None


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


class LenientUndefined(ChainableUndefined):
    """Undefined that swallows macro calls and other undefined usage."""

    __slots__ = ()

    def __call__(self, *args: Any, **kwargs: Any) -> "LenientUndefined":
        return self

    def __len__(self) -> int:  # pragma: no cover - defensive for safety
        return 0


class JinjaTemplateParser:
    """Handles parsing and rendering of Jinja2 templates"""

    def __init__(self) -> None:
        self._env = ImmutableSandboxedEnvironment(undefined=LenientUndefined)

    def extract_jinja_variables(self, sql_template: str) -> list[ModelVariable]:
        """
        Extract Jinja variables from SQL template by attempting to render it.

        Looks for patterns like:
        - {{ var('variable_name') }}
        - {{ var('variable_name', 'default_value') }}

        Args:
            sql_template: The SQL template string with Jinja syntax

        Returns:
            List of ModelVariable objects
        """
        variables: dict[str, ModelVariable] = {}

        # Create a tracking var() function that records calls
        def tracking_var(name: str, default: Any = None) -> str:
            """Track var() calls during template rendering."""
            if name not in variables:
                variables[name] = ModelVariable(
                    name=name,
                    default_value=default,
                )
            return str(default) if default is not None else ""

        # Attempt to render the template with our tracking function
        # Other undefined functions/variables will be handled by LenientUndefined
        template = self._env.from_string(sql_template)
        template.render(var=tracking_var)
        return list(variables.values())


def discover_tool_models(
    project_dir: str,
    tools_subdir: str,
    dbt_path: str,
    parser: JinjaTemplateParser,
    fs_provider: FileSystemProvider,
) -> list[CustomToolModel]:
    """
    Discover custom tool models in the dbt project.

    Args:
        project_dir: Path to the dbt project root directory
        tools_subdir: Subdirectory containing tool models
        dbt_path: Path to the dbt executable
        parser: JinjaTemplateParser instance for extracting variables
        fs_provider: File system provider for reading files

    Returns:
        List of CustomToolModel objects
    """

    models: list[CustomToolModel] = []
    # Run dbt ls to get all models in the tools directory
    # --output json gives us structured data
    # --output-keys specifies what fields to include
    command = [
        dbt_path,
        "ls",
        "--quiet",
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
    result = subprocess.run(
        command,
        cwd=project_dir,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    if result.returncode != 0:
        print("here 1")
        print(result.stderr)
        logger.error(f"dbt ls command failed: {result.stderr}")
        return []

    # Parse JSON output - each line is a separate JSON object
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue

        try:
            print("here 0")
            print(line)
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
            print("here 2")
            print(e)
            logger.error(
                "Failed to parse dbt JSON output line: %s",
                e,
            )
            continue
        except Exception as e:
            print("here 3")
            print(e)
            logger.error(f"Failed to process model: {e}")
            continue

    return models
