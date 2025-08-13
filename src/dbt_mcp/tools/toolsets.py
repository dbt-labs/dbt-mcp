from enum import Enum

from dbt_mcp.tools.tool_names import ToolName


class Toolset(Enum):
    SQL = "sql"
    SEMANTIC_LAYER = "semantic_layer"
    DISCOVERY = "discovery"
    DBT_CLI = "dbt_cli"


toolsets = {
    Toolset.SQL: {
        ToolName.TEXT_TO_SQL,
        ToolName.EXECUTE_SQL,
    },
    Toolset.SEMANTIC_LAYER: {
        ToolName.LIST_METRICS,
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
    },
}
