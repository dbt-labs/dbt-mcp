"""Custom tools module for dbt-mcp.

This module provides functionality for discovering and registering custom tools
from dbt models in a tools directory.
"""

from dbt_mcp.custom_tools.filesystem import (
    FileSystemProvider,
    LocalFileSystemProvider,
)
from dbt_mcp.custom_tools.model_discovery import (
    CustomToolModel,
    JinjaTemplateParser,
    ModelVariable,
    discover_tool_models,
)
from dbt_mcp.custom_tools.tools import (
    create_custom_tool_definitions,
    register_custom_tools,
)

__all__ = [
    # Filesystem providers
    "FileSystemProvider",
    "LocalFileSystemProvider",
    # Model discovery
    "CustomToolModel",
    "JinjaTemplateParser",
    "ModelVariable",
    "discover_tool_models",
    # Tool registration
    "create_custom_tool_definitions",
    "register_custom_tools",
]
