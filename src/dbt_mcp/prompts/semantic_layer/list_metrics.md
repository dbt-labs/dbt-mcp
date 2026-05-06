List metrics from the dbt Semantic Layer.

The response is a CSV string with a header row. Columns are dynamic: a column is only present if at least one metric has a non-empty value for it. `name` and `type` are always present; `label`, `description`, `metadata`, `dimensions`, and `entities` are included only when at least one metric has a value. The `dimensions` and `entities` cells contain comma-separated lists of names.

When the number of metrics is below the configured threshold (default: 10), each metric includes the names of its available dimensions and entities. Use get_dimensions or get_entities for full details (types, granularities, descriptions) on specific metrics.

When above the threshold, only metrics are returned. `metric_time` is a standard time dimension available on most metrics — you can often query directly without calling `get_dimensions` first. Call `get_dimensions` only when you need non-time dimensions or specific granularity details.

If the user is asking a data-related or business-related question, use this tool as a first step.

