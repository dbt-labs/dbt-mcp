# %%
from enum import Enum, StrEnum, unique

@unique
class ToolGroup(StrEnum):
    DBT_CLI = "dbt_cli"
    SEMANTIC_LAYER = "semantic_layer"
    DISCOVERY = "discovery"
    SQL = "sql"
    ADMIN_API = "admin_api"
    DBT_CODEGEN = "dbt_codegen"

class ToolName(Enum):
    """Tool names available in the FastMCP server.

    This enum provides type safety and autocompletion for tool names.
    The validate_server_tools() function should be used to ensure
    this enum stays in sync with the actual server tools.
    """
    def __new__(cls, value:str, group: ToolGroup):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.group = group
        return obj

    # dbt CLI tools
    BUILD = ("build", ToolGroup.DBT_CLI)
    COMPILE = ("compile", ToolGroup.DBT_CLI)
    DOCS = ("docs", ToolGroup.DBT_CLI)
    LIST = ("list", ToolGroup.DBT_CLI)
    PARSE = ("parse", ToolGroup.DBT_CLI)
    RUN = ("run", ToolGroup.DBT_CLI)
    TEST = ("test", ToolGroup.DBT_CLI)
    SHOW = ("show", ToolGroup.DBT_CLI)

    # Semantic Layer tools
    LIST_METRICS = ("list_metrics", ToolGroup.SEMANTIC_LAYER)
    GET_DIMENSIONS = ("get_dimensions", ToolGroup.SEMANTIC_LAYER)
    GET_ENTITIES = ("get_entities", ToolGroup.SEMANTIC_LAYER)
    QUERY_METRICS = ("query_metrics", ToolGroup.SEMANTIC_LAYER)
    GET_METRICS_COMPILED_SQL = ("get_metrics_compiled_sql", ToolGroup.SEMANTIC_LAYER)

    # Discovery tools
    GET_MART_MODELS = ("get_mart_models", ToolGroup.DISCOVERY)
    GET_ALL_MODELS = ("get_all_models", ToolGroup.DISCOVERY)
    GET_MODEL_DETAILS = ("get_model_details", ToolGroup.DISCOVERY)
    GET_MODEL_PARENTS = ("get_model_parents", ToolGroup.DISCOVERY)
    GET_MODEL_CHILDREN = ("get_model_children", ToolGroup.DISCOVERY)
    GET_MODEL_HEALTH = ("get_model_health", ToolGroup.DISCOVERY)
    GET_EXPOSURES = ("get_exposures", ToolGroup.DISCOVERY)
    GET_EXPOSURE_DETAILS = ("get_exposure_details", ToolGroup.DISCOVERY)

    # SQL tools
    TEXT_TO_SQL = ("text_to_sql", ToolGroup.SQL)
    EXECUTE_SQL = ("execute_sql", ToolGroup.SQL)

    # Admin API tools
    LIST_JOBS = ("list_jobs", ToolGroup.ADMIN_API)
    GET_JOB_DETAILS = ("get_job_details", ToolGroup.ADMIN_API)
    TRIGGER_JOB_RUN = ("trigger_job_run", ToolGroup.ADMIN_API)
    LIST_JOBS_RUNS = ("list_jobs_runs", ToolGroup.ADMIN_API)
    GET_JOB_RUN_DETAILS = ("get_job_run_details", ToolGroup.ADMIN_API)
    CANCEL_JOB_RUN = ("cancel_job_run", ToolGroup.ADMIN_API)
    RETRY_JOB_RUN = ("retry_job_run", ToolGroup.ADMIN_API)
    LIST_JOB_RUN_ARTIFACTS = ("list_job_run_artifacts", ToolGroup.ADMIN_API)
    GET_JOB_RUN_ARTIFACT = ("get_job_run_artifact", ToolGroup.ADMIN_API)
    GET_JOB_RUN_ERROR = ("get_job_run_error", ToolGroup.ADMIN_API)

    # dbt-codegen tools
    GENERATE_SOURCE = ("generate_source", ToolGroup.DBT_CODEGEN)
    GENERATE_MODEL_YAML = ("generate_model_yaml", ToolGroup.DBT_CODEGEN)
    GENERATE_STAGING_MODEL = ("generate_staging_model", ToolGroup.DBT_CODEGEN)

    @classmethod
    def get_all_tool_names(cls) -> set[str]:
        """Returns a set of all tool names as strings."""
        return {member.value for member in cls}

    @classmethod
    def get_tools_by_group(cls, group: ToolGroup) -> set["ToolName"]:
        return {member for member in cls if member.group is group}
