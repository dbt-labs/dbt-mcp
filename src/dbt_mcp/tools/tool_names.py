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
    LIST_SAVED_QUERIES = "list_saved_queries"
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
    GET_MODEL_HEALTH = "get_model_health"
    GET_ALL_SOURCES = "get_all_sources"
    GET_EXPOSURES = "get_exposures"
    GET_EXPOSURE_DETAILS = "get_exposure_details"

    # SQL tools
    TEXT_TO_SQL = "text_to_sql"
    EXECUTE_SQL = "execute_sql"

    # Admin API tools
    LIST_JOBS = "list_jobs"
    GET_JOB_DETAILS = "get_job_details"
    TRIGGER_JOB_RUN = "trigger_job_run"
    LIST_JOBS_RUNS = "list_jobs_runs"
    GET_JOB_RUN_DETAILS = "get_job_run_details"
    CANCEL_JOB_RUN = "cancel_job_run"
    RETRY_JOB_RUN = "retry_job_run"
    LIST_JOB_RUN_ARTIFACTS = "list_job_run_artifacts"
    GET_JOB_RUN_ARTIFACT = "get_job_run_artifact"
    GET_JOB_RUN_ERROR = "get_job_run_error"

    # dbt-codegen tools
    GENERATE_SOURCE = "generate_source"
    GENERATE_MODEL_YAML = "generate_model_yaml"
    GENERATE_STAGING_MODEL = "generate_staging_model"

    # dbt LSP tools
    GET_COLUMN_LINEAGE = "get_column_lineage"

    @classmethod
    def get_all_tool_names(cls) -> set[str]:
        """Returns a set of all tool names as strings."""
        return {member.value for member in cls}
