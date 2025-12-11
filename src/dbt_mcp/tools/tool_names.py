from enum import Enum


class ToolName(Enum):
    """Tool names available in the FastMCP server."""

    # dbt CLI tools
    BUILD = "build"
    COMPILE = "compile"
    DOCS = "docs"
    LIST = "list"
    PARSE = "parse"
    RUN = "run"
    TEST = "test"
    SHOW = "show"
    GET_MODEL_LINEAGE_DEV = "get_model_lineage_dev"

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
    GET_SOURCE_DETAILS = "get_source_details"
    GET_EXPOSURES = "get_exposures"
    GET_EXPOSURE_DETAILS = "get_exposure_details"
    GET_RELATED_MODELS = "get_related_models"
    GET_MACRO_DETAILS = "get_macro_details"
    GET_SEED_DETAILS = "get_seed_details"
    GET_SEMANTIC_MODEL_DETAILS = "get_semantic_model_details"
    GET_SNAPSHOT_DETAILS = "get_snapshot_details"
    GET_TEST_DETAILS = "get_test_details"
    SEARCH = "search"  # The search tool is not generally available yet

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
    FUSION_COMPILE_SQL = "fusion.compile_sql"
    FUSION_GET_COLUMN_LINEAGE = "fusion.get_column_lineage"

    @classmethod
    def get_all_tool_names(cls) -> set[str]:
        """Returns a set of all tool names as strings."""
        return {member.value for member in cls}
