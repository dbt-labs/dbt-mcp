List all loaded dbt artifact tables and their row counts.

Use this as your first call after loading artifacts to see what data is available. Returns an empty list if no artifacts have been loaded yet.

## Parameters

None.

## Returns

A list of dicts, each with:
- `table_name`: Name of the table (e.g. "nodes", "edges", "run_results")
- `row_count`: Number of rows in the table

## Example Response

```json
[
  {"table_name": "nodes", "row_count": 5690},
  {"table_name": "node_columns", "row_count": 19407},
  {"table_name": "edges", "row_count": 6541},
  {"table_name": "run_results", "row_count": 4743}
]
```
