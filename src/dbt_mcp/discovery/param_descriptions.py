"""JSON Schema parameter descriptions for Discovery MCP tools."""

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

MESH_SEARCH_QUERY_DESCRIPTION = (
    "Optional search text matched as a case-insensitive substring against "
    "model names and descriptions, e.g. 'gold finance sales'. Not semantic "
    "search. Omit to list everything in scope. Alongside `relation` it "
    "only filters that one relation's model in or out; it never broadens the "
    "search."
)

MESH_RELATION_DESCRIPTION = (
    "Exact warehouse relation to look up, as separate fields rather than one "
    "dotted string -- provide `identifier` (required), and `schema`/"
    "`database` to narrow further when known. Each supplied field is matched "
    "exactly (not a prefix or substring) and case-insensitively. Read the "
    "answer from the returned `lookup` field."
)

MESH_PROJECT_IDS_DESCRIPTION = (
    "dbt platform project IDs of the producer projects to search for public "
    "Mesh models. Must be a subset of the IDs configured via DBT_PROJECT_IDS"
    ", or chosen during multi-project OAuth selection."
)

MESH_LIMIT_DESCRIPTION = (
    "Maximum number of matching models to return (1-100, default 50). Caps "
    "the merged list across all searched projects, not a single GraphQL page; "
    "there is no cursor to fetch more. `lookup`, `coverage`, and "
    "`dependencies` are computed before this cap applies."
)

# Not a JSON Schema param description — used as the `arg_mapping` for the
# get_model_parents/get_model_children deprecation banner. get_lineage requires
# unique_id (these tools accept name alone), and its default node limit can
# omit direct parents or children.
GET_MODEL_PARENTS_ARG_MAPPING = (
    'Call get_lineage(unique_id=..., depth=1, direction="upstream") — it '
    'requires unique_id (not name); direction="upstream" '
    "returns parents up to the node limit. Check omitted_node_count for more."
)

GET_MODEL_CHILDREN_ARG_MAPPING = (
    'Call get_lineage(unique_id=..., depth=1, direction="downstream") — it '
    'requires unique_id (not name); direction="downstream" '
    "returns children up to the node limit. Check omitted_node_count for more."
)
