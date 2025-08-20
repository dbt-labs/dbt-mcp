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
    GET_MODEL_HEALTH = "get_model_health"

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

    @classmethod
    def get_all_tool_names(cls) -> set[str]:
        """Returns a set of all tool names as strings."""
        return {member.value for member in cls}
