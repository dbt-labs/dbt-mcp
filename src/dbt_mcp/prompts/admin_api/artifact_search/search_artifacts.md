Full-text BM25 keyword search across loaded dbt artifact tables.

Searches names, descriptions, SQL code, column metadata, and other text fields using BM25 ranking. Use this for natural language queries like "find the customer model" or "orders table". Results are ranked by relevance.

## Parameters

- **table_name** (required): Table to search. Tables with FTS support:
  - `nodes` — searches name, description, raw_code, compiled_code
  - `node_columns` — searches column_name, description
  - `exposures` — searches name, description
  - `metrics` — searches name, label, description
  - `macros` — searches name, description, macro_sql
  - `catalog_tables` — searches table_name, table_comment
  - `run_results` — searches status, message
  - `source_freshness` — searches source_name, table_name, status
- **search_query** (required): Keyword search terms (e.g. "customer orders", "failed error").
- **limit** (optional, default: 20): Maximum number of results.

## Returns

A list of dicts (table rows) ranked by BM25 relevance score. Each result includes all columns from the table plus an `fts_score` field.

## Example Usage

```json
{"table_name": "nodes", "search_query": "customer dimension", "limit": 10}
```
