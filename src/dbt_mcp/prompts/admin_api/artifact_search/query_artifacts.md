Execute a read-only SQL query against loaded dbt artifact data in the in-memory database.

Supports full SQL including JOINs, aggregations, CTEs, window functions, and subqueries. Results are capped at 500 rows. Only SELECT queries are allowed.

## Parameters

- **sql** (required): A read-only SQL query. Must start with SELECT (or WITH for CTEs).

## Returns

A list of dicts, one per row, with column names as keys.

## Key Tables and Relationships

- `nodes` — All models, tests, seeds, snapshots, sources (join with `node_columns` on `unique_id`)
- `edges` — Dependency graph: `parent_unique_id` → `child_unique_id` (use for impact analysis)
- `node_columns` — Column metadata with manifest descriptions + catalog types
- `run_results` — Per-node execution status, timing, error messages
- `test_metadata` — Test configurations with `attached_node` for the model being tested

## Example Queries

Find failed models:
```sql
SELECT unique_id, message FROM run_results WHERE status = 'error'
```

Impact analysis (what depends on a model):
```sql
SELECT child_unique_id FROM edges WHERE parent_unique_id = 'model.analytics.dim_customers'
```

Models with their column counts:
```sql
SELECT n.name, n.resource_type, COUNT(nc.column_name) as col_count
FROM nodes n LEFT JOIN node_columns nc ON n.unique_id = nc.unique_id
WHERE n.resource_type = 'model'
GROUP BY n.name, n.resource_type ORDER BY col_count DESC LIMIT 20
```
