"""JSON Schema parameter descriptions for dbt Admin API MCP tools."""

# --- Shared across tools ---

PAGINATION_LIMIT = "Maximum number of results to return"
PAGINATION_OFFSET = "Number of results to skip for pagination"

JOB_DEFINITION_ID = "The dbt job definition ID"
JOB_RUN_ID = "The dbt job run ID"

# --- list_jobs_runs ---

JOB_RUNS_JOB_DEFINITION_ID_FILTER = (
    "When set, only include runs for this job definition ID"
)
JOB_RUN_STATUS = (
    "Filter by run status: queued, starting, running, success, error, or cancelled"
)
JOB_RUNS_ORDER_BY = (
    'Sort field (e.g. "created_at", "finished_at", "id"); prefix with "-" '
    'for descending order (e.g. "-created_at" for newest first)'
)

# --- trigger_job_run ---

TRIGGER_CAUSE = (
    "Why this run is being triggered (recorded in run history on the dbt platform)"
)
TRIGGER_GIT_BRANCH = "Override the Git branch to check out for this run"
TRIGGER_GIT_SHA = "Override the Git commit SHA to check out for this run"
TRIGGER_SCHEMA_OVERRIDE = "Override the destination schema for this run"
TRIGGER_STEPS_OVERRIDE = (
    "Replace the job's default dbt commands; each entry is a full dbt command "
    '(e.g. "dbt run --select my_model+ --full-refresh")'
)

# --- get_job_run_error ---

INCLUDE_WARNINGS_WITH_ERRORS = (
    "If true, include warning analysis together with error details"
)
WARNINGS_ONLY = (
    "If true, return only warning analysis (e.g. for successful runs with warnings)"
)
