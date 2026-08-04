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
TRIGGER_DBT_VERSION_OVERRIDE = (
    "Override the dbt version this run executes on, without changing the job or "
    'its environment. Accepts a release track (e.g. "latest", "compatible", '
    '"extended") or a pinned version (e.g. "1.9.0")'
)

# --- get_job_run_artifacts ---

ARTIFACT_PATH = (
    "Path to the artifact file (e.g. 'manifest.json', 'run_results.json', "
    "'catalog.json'). Use list_job_run_artifacts to see available paths."
)
ARTIFACT_STEP = (
    "Step number to retrieve the artifact from (1-indexed). "
    "Defaults to the last step when omitted. "
    "Only dbt command steps (dbt build, dbt run, dbt test, dbt docs generate) "
    "produce artifacts — infrastructure steps (git clone, profile creation, dbt deps) "
    "will raise a not-found error."
)
ARTIFACT_JQ_FILTER = (
    "A jq filter expression to apply to the artifact before returning. "
    "Only valid for JSON artifacts. Results are always returned as a JSON array inline "
    "regardless of artifact size — if the filter matches nothing, '[]' is returned. "
    "Examples: '.results[] | select(.status == \"error\")' to extract failures; "
    "'.nodes | keys[]' to list all node IDs; "
    "'.metadata' to inspect artifact metadata."
)

# --- get_job_run_error ---

INCLUDE_WARNINGS_WITH_ERRORS = (
    "If true, include warning analysis together with error details"
)
WARNINGS_ONLY = (
    "If true, return only warning analysis (e.g. for successful runs with warnings)"
)
