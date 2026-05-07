"""JSON Schema parameter descriptions and Field() defaults for Discovery MCP tools."""

from pydantic import Field

DISCOVERY_PROJECT_ID_DESCRIPTION = (
    "The dbt Cloud project ID to query. "
    "Use list_projects_and_environments to discover available project IDs."
)

PROJECT_ID_FIELD = Field(description=DISCOVERY_PROJECT_ID_DESCRIPTION)

SOURCE_NAMES_FILTER = (
    "Filter by top-level source names from the project `sources:` YAML "
    "(e.g. `raw_data`, `external_api`)"
)

SOURCE_UNIQUE_IDS_FILTER = (
    "Filter by fully qualified source table unique IDs "
    "(`source.<project>.<source_name>.<table>`)"
)

MODEL_PERF_NUM_RUNS = (
    "Number of historical runs to return (1–100). Default 1 returns the latest run only; "
    "use higher values to analyze performance trends."
)

MODEL_PERF_INCLUDE_TESTS = (
    "When true, include test execution history (name, status, execution time) for each run; "
    "when false, omit tests to keep the response smaller."
)

MACRO_PACKAGE_NAMES = (
    "Filter macros to these package names (e.g. `my_project`, `dbt_utils`)"
)

MACRO_RETURN_PACKAGE_NAMES_ONLY = "When true, return only distinct package names (use to discover packages before filtering)"

MACRO_INCLUDE_DEFAULT_DBT_PACKAGES = (
    "When true, include default dbt Labs core/adapter macro packages"
)
