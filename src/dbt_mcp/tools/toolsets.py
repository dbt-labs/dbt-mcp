"""Toolset definitions and tool-to-toolset mappings.

This module defines the toolsets available in dbt-mcp and provides
a mapping from individual tools to their respective toolsets. This
enables toolset-level enablement/disablement configuration.
"""

from enum import Enum
from typing import Literal

from dbt_mcp.tools.tool_names import ToolName


class Toolset(Enum):
    SQL = "sql"
    SEMANTIC_LAYER = "semantic_layer"
    DISCOVERY = "discovery"
    DBT_CLI = "dbt_cli"
    ADMIN_API = "admin_api"
    DBT_CODEGEN = "dbt_codegen"
    DBT_LSP = "dbt_lsp"


proxied_tools: set[
    Literal[
        ToolName.TEXT_TO_SQL,
        ToolName.EXECUTE_SQL,
        ToolName.GET_RELATED_MODELS,
        ToolName.SEARCH,
        ToolName.FUSION_COMPILE_SQL,
        ToolName.FUSION_GET_COLUMN_LINEAGE,
    ]
] = set(
    [
        ToolName.TEXT_TO_SQL,
        ToolName.EXECUTE_SQL,
        ToolName.GET_RELATED_MODELS,
        ToolName.SEARCH,
        ToolName.FUSION_COMPILE_SQL,
        ToolName.FUSION_GET_COLUMN_LINEAGE,
    ]
)

toolsets = {
    Toolset.SQL: {
        ToolName.TEXT_TO_SQL,
        ToolName.EXECUTE_SQL,
    },
    Toolset.SEMANTIC_LAYER: {
        ToolName.LIST_METRICS,
        ToolName.LIST_SAVED_QUERIES,
        ToolName.GET_DIMENSIONS,
        ToolName.GET_ENTITIES,
        ToolName.QUERY_METRICS,
        ToolName.GET_METRICS_COMPILED_SQL,
    },
    Toolset.DISCOVERY: {
        ToolName.GET_MART_MODELS,
        ToolName.GET_ALL_MODELS,
        ToolName.GET_MODEL_DETAILS,
        ToolName.GET_MODEL_PARENTS,
        ToolName.GET_MODEL_CHILDREN,
        ToolName.GET_MODEL_HEALTH,
        ToolName.GET_ALL_SOURCES,
        ToolName.GET_SOURCE_DETAILS,
        ToolName.GET_EXPOSURES,
        ToolName.GET_EXPOSURE_DETAILS,
        ToolName.GET_RELATED_MODELS,
        ToolName.GET_MACRO_DETAILS,
        ToolName.GET_SEED_DETAILS,
        ToolName.GET_SEMANTIC_MODEL_DETAILS,
        ToolName.GET_SNAPSHOT_DETAILS,
        ToolName.GET_TEST_DETAILS,
        ToolName.SEARCH,
    },
    Toolset.DBT_CLI: {
        ToolName.BUILD,
        ToolName.COMPILE,
        ToolName.DOCS,
        ToolName.LIST,
        ToolName.PARSE,
        ToolName.RUN,
        ToolName.TEST,
        ToolName.SHOW,
        ToolName.GET_MODEL_LINEAGE_DEV,
    },
    Toolset.ADMIN_API: {
        ToolName.LIST_JOBS,
        ToolName.GET_JOB_DETAILS,
        ToolName.TRIGGER_JOB_RUN,
        ToolName.LIST_JOBS_RUNS,
        ToolName.GET_JOB_RUN_DETAILS,
        ToolName.CANCEL_JOB_RUN,
        ToolName.RETRY_JOB_RUN,
        ToolName.LIST_JOB_RUN_ARTIFACTS,
        ToolName.GET_JOB_RUN_ARTIFACT,
        ToolName.GET_JOB_RUN_ERROR,
    },
    Toolset.DBT_CODEGEN: {
        ToolName.GENERATE_SOURCE,
        ToolName.GENERATE_MODEL_YAML,
        ToolName.GENERATE_STAGING_MODEL,
    },
    Toolset.DBT_LSP: {
        ToolName.GET_COLUMN_LINEAGE,
        ToolName.FUSION_COMPILE_SQL,
        ToolName.FUSION_GET_COLUMN_LINEAGE,
    },
}


# Mapping from individual tools to their toolsets (for precedence logic)
TOOL_TO_TOOLSET: dict[ToolName, Toolset] = {}
for toolset, tools in toolsets.items():
    for tool in tools:
        TOOL_TO_TOOLSET[tool] = toolset


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
