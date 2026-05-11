"""JSON Schema parameter descriptions for Semantic Layer MCP tools."""

from dbt_mcp.dbt_admin.param_descriptions import PAGINATION_LIMIT
from dbt_mcp.tools.multiproject_params import MULTI_PROJECT_PROJECT_ID_DESCRIPTION

# Reuse Admin API wording for semantic query row limits (single source of truth).
QUERY_RESULT_LIMIT = PAGINATION_LIMIT

SEMANTIC_LAYER_PROJECT_ID = MULTI_PROJECT_PROJECT_ID_DESCRIPTION

SEMANTIC_SEARCH_METRICS = (
    "Filter metrics by substring match against the metric name. "
    "Accepts either a single substring or a list of substrings; when a list "
    "is provided, metrics matching any of the substrings are returned "
    "(deduplicated)."
)

SEMANTIC_SEARCH_SAVED_QUERIES = (
    "Filter saved queries by substring match on name, label, or description"
)

SEMANTIC_METRICS = "Metric names from list_metrics to query or inspect"

SEMANTIC_SEARCH_DIMENSIONS = "Filter dimensions by substring match on name (omit to return all for these metrics)"

SEMANTIC_SEARCH_ENTITIES = (
    "Filter entities by substring match on name (omit to return all for these metrics)"
)

SEMANTIC_GROUP_BY = (
    "Group by dimensions, time dimensions, or entities; each item has "
    '`name`, `type` (`"dimension"`, `"time_dimension"`, or `"entity"`), '
    "and optional `grain` for time dimensions"
)

SEMANTIC_ORDER_BY = (
    "Sort keys; each item has `name` and `descending` (default false); "
    "order fields should appear in group_by when grouping"
)

SEMANTIC_WHERE = (
    "Semantic Layer filter; use {{ Dimension('name') }}, "
    "{{ TimeDimension('name', 'grain') }}, {{ Entity('name') }}; dates as yyyy-mm-dd"
)
