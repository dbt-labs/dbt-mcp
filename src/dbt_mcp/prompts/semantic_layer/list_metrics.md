List metrics from the dbt Semantic Layer.

The response is a CSV string with a header row. Columns are dynamic: a column is only present if at least one metric has a non-empty value for it. `name` and `type` are always present; `label`, `description`, `metadata`, `dimensions`, and `entities` are included only when at least one metric has a value. The `dimensions` and `entities` cells contain comma-separated lists of names.

When the number of metrics is below the configured threshold (default: 10), each metric includes the names of its available dimensions and entities. Use get_dimensions or get_entities for full details (types, granularities, descriptions) on specific metrics.

When above the threshold, only metrics are returned. `metric_time` is a standard time dimension available on most metrics — you can often query directly without calling `get_dimensions` first. Call `get_dimensions` only when you need non-time dimensions or specific granularity details.

For broad listings that exceed the size budget, columns are dropped progressively to save tokens: `description` is dropped first; `metadata` is dropped only if removing `description` alone is not enough. The CSV is prefixed with one or more `# Note:` lines listing which columns were omitted. When that happens, call `list_metrics` again with the `search` parameter to retrieve those fields for the specific metrics you care about — a narrow result set (at or below the related-metrics threshold) is always returned with full `description` and `metadata`, even if the text is verbose. `search` accepts either a single substring or a list of substrings; when a list is provided, metrics whose name matches **any** of the substrings are returned (deduplicated), so you can fetch details for several metrics in one call.

`meta_filter` accepts a dict of key-value pairs and restricts results to metrics whose `config.meta` contains all specified pairs — for example `{"agent_accessible": true}`. Apply it on broad searches to keep the result set small enough that the `metadata` column is preserved in the response.

If the user is asking a data-related or business-related question, use this tool as a first step.
