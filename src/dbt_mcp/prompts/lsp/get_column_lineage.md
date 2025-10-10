Get the column-level lineage for a specific column in a dbt model.

This tool traces how a column's value is derived from upstream sources through transformations, showing the complete lineage path from source tables to the target column. It's useful for understanding data flow, debugging data quality issues, and impact analysis.

The lineage includes:
- Upstream columns that contribute to this column
- SQL transformations applied
- Intermediate models in the lineage path
- Source tables and columns

Use this when you need to understand where a column's data comes from or which upstream changes might affect it.
