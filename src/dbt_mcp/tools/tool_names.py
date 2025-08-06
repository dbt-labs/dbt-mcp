from enum import Enum


class ToolName(Enum):
    """Tool names available in the FastMCP server.

    This enum provides type safety and autocompletion for tool names.
    The validate_server_tools() function should be used to ensure
    this enum stays in sync with the actual server tools.
    """

    # dbt CLI tools
    BUILD = "build"
    COMPILE = "compile"
    DOCS = "docs"
    LIST = "list"
    PARSE = "parse"
    RUN = "run"
    TEST = "test"
    SHOW = "show"

    # Semantic Layer tools
    LIST_METRICS = "list_metrics"
    GET_DIMENSIONS = "get_dimensions"
    GET_ENTITIES = "get_entities"
    QUERY_METRICS = "query_metrics"
    GET_METRICS_COMPILED_SQL = "get_metrics_compiled_sql"

    # Discovery tools
    GET_MART_MODELS = "get_mart_models"
    GET_ALL_MODELS = "get_all_models"
    GET_MODEL_DETAILS = "get_model_details"
    GET_MODEL_PARENTS = "get_model_parents"
    GET_MODEL_CHILDREN = "get_model_children"

    # SQL tools
    TEXT_TO_SQL = "text_to_sql"
    EXECUTE_SQL = "execute_sql"

    @classmethod
    def get_all_tool_names(cls) -> set[str]:
        """Returns a set of all tool names as strings."""
        return {member.value for member in cls}
