"""JSON Schema parameter descriptions for Discovery MCP tools."""

from dbt_mcp.tools.multiproject_params import MULTI_PROJECT_PROJECT_ID_DESCRIPTION

DISCOVERY_PROJECT_ID_DESCRIPTION = MULTI_PROJECT_PROJECT_ID_DESCRIPTION

RESOURCE_TYPE_DESCRIPTION = (
    "The type of dbt resource to fetch details for. "
    "One of: model, source, exposure, test, seed, snapshot, macro, semantic_model. "
    "Tip: the resource_type is encoded in the unique_id prefix "
    "(e.g. `model.my_project.orders` → model)."
)

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

# Not a JSON Schema param description — used as the `arg_mapping` for the
# get_model_parents/get_model_children deprecation banner. get_lineage isn't a
# drop-in on its own: it requires unique_id (these tools accept name alone) and
# defaults to depth=5. The `direction` param (upstream/downstream/both) makes it
# a true drop-in once both differences are called out.
GET_MODEL_PARENTS_ARG_MAPPING = (
    'Call get_lineage(unique_id=..., depth=1, direction="upstream") — it '
    'requires unique_id (not name) and defaults to depth=5; direction="upstream" '
    "returns only parents, matching this tool's behavior."
)

GET_MODEL_CHILDREN_ARG_MAPPING = (
    'Call get_lineage(unique_id=..., depth=1, direction="downstream") — it '
    'requires unique_id (not name) and defaults to depth=5; direction="downstream" '
    "returns only children, matching this tool's behavior."
)
