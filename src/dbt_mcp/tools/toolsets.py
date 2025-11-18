"""Toolset definitions and tool-to-toolset mappings.

This module defines the toolsets available in dbt-mcp and provides
a mapping from individual tools to their respective toolsets. This
enables a toolset-level enablement/disablement configuration.
"""

from enum import Enum

from dbt_mcp.tools.tool_names import ToolName


class Toolset(str, Enum):
    """Available toolsets in dbt-mcp.
    
    Each toolset represents a logical grouping of related tools.
    """

    SEMANTIC_LAYER = "SEMANTIC_LAYER"
    ADMIN_API = "ADMIN_API"
    CLI = "CLI"
    CODEGEN = "CODEGEN"
    DISCOVERY = "DISCOVERY"
    LSP = "LSP"
    SQL = "SQL"


# Mapping from individual tools to their toolsets
TOOL_TO_TOOLSET: dict[ToolName, Toolset] = {
    # Semantic Layer tools
    ToolName.LIST_METRICS: Toolset.SEMANTIC_LAYER,
    ToolName.LIST_SAVED_QUERIES: Toolset.SEMANTIC_LAYER,
    ToolName.GET_DIMENSIONS: Toolset.SEMANTIC_LAYER,
    ToolName.GET_ENTITIES: Toolset.SEMANTIC_LAYER,
    ToolName.QUERY_METRICS: Toolset.SEMANTIC_LAYER,
    ToolName.GET_METRICS_COMPILED_SQL: Toolset.SEMANTIC_LAYER,
    # Admin API tools
    ToolName.LIST_JOBS: Toolset.ADMIN_API,
    ToolName.GET_JOB_DETAILS: Toolset.ADMIN_API,
    ToolName.TRIGGER_JOB_RUN: Toolset.ADMIN_API,
    ToolName.LIST_JOBS_RUNS: Toolset.ADMIN_API,
    ToolName.GET_JOB_RUN_DETAILS: Toolset.ADMIN_API,
    ToolName.CANCEL_JOB_RUN: Toolset.ADMIN_API,
    ToolName.RETRY_JOB_RUN: Toolset.ADMIN_API,
    ToolName.LIST_JOB_RUN_ARTIFACTS: Toolset.ADMIN_API,
    ToolName.GET_JOB_RUN_ARTIFACT: Toolset.ADMIN_API,
    ToolName.GET_JOB_RUN_ERROR: Toolset.ADMIN_API,
    # CLI tools
    ToolName.BUILD: Toolset.CLI,
    ToolName.COMPILE: Toolset.CLI,
    ToolName.DOCS: Toolset.CLI,
    ToolName.LIST: Toolset.CLI,
    ToolName.PARSE: Toolset.CLI,
    ToolName.RUN: Toolset.CLI,
    ToolName.TEST: Toolset.CLI,
    ToolName.SHOW: Toolset.CLI,
    # Codegen tools
    ToolName.GENERATE_SOURCE: Toolset.CODEGEN,
    ToolName.GENERATE_MODEL_YAML: Toolset.CODEGEN,
    ToolName.GENERATE_STAGING_MODEL: Toolset.CODEGEN,
    # Discovery tools
    ToolName.GET_MART_MODELS: Toolset.DISCOVERY,
    ToolName.GET_ALL_MODELS: Toolset.DISCOVERY,
    ToolName.GET_MODEL_DETAILS: Toolset.DISCOVERY,
    ToolName.GET_MODEL_PARENTS: Toolset.DISCOVERY,
    ToolName.GET_MODEL_CHILDREN: Toolset.DISCOVERY,
    ToolName.GET_MODEL_HEALTH: Toolset.DISCOVERY,
    ToolName.GET_ALL_SOURCES: Toolset.DISCOVERY,
    ToolName.GET_SOURCE_DETAILS: Toolset.DISCOVERY,
    ToolName.GET_EXPOSURES: Toolset.DISCOVERY,
    ToolName.GET_EXPOSURE_DETAILS: Toolset.DISCOVERY,
    ToolName.GET_RELATED_MODELS: Toolset.DISCOVERY,
    ToolName.KEYWORD_SEARCH: Toolset.DISCOVERY,
    # LSP tools
    ToolName.GET_COLUMN_LINEAGE: Toolset.LSP,
    # SQL tools (proxied)
    ToolName.TEXT_TO_SQL: Toolset.SQL,
    ToolName.EXECUTE_SQL: Toolset.SQL,
}



def validate_tool_mapping() -> None:
    """Ensure all ToolName members are mapped to a toolset.
    
    Raises:
        ValueError: If any tools are not mapped to a toolset
    """
    unmapped = set(ToolName) - set(TOOL_TO_TOOLSET.keys())
    if unmapped:
        unmapped_names = [tool.value for tool in unmapped]
        raise ValueError(
            f"The following tools are not mapped to toolsets: {', '.join(unmapped_names)}"
        )


# Validate at import time to catch errors early
validate_tool_mapping()
