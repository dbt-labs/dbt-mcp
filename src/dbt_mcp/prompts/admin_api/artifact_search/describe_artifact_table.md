Show column names and types for a loaded artifact table.

Use this to understand the schema of a table before writing SQL queries. Call `list_artifact_tables` first to see which tables are available.

## Parameters

- **table_name** (required): Name of the table to describe (e.g. "nodes", "edges", "run_results").

## Returns

A list of dicts, each with:
- `column_name`: Name of the column
- `column_type`: DuckDB type (e.g. VARCHAR, INTEGER, BOOLEAN, FLOAT, TEXT)

## Example Usage

```json
{"table_name": "nodes"}
```
