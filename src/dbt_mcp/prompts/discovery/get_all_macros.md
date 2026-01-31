Get the name, description, and package name of all dbt macros in the environment. Macros are reusable pieces of Jinja SQL that can be called from models, tests, and other macros.

Parameters (all optional):
- package_names: List of specific package names to filter by (e.g., ['my_project', 'dbt_utils'])
- return_package_names_only: If True, returns only unique package names (useful to discover packages first)

Usage patterns:
1. Discover available packages first: `get_all_macros(return_package_names_only=True)`
2. Then get macros for specific packages: `get_all_macros(package_names=['my_project'])`

Note:
  - dbt-labs first-party packages are automatically excluded. These are packages maintained directly by dbt Labs from the dbt-labs/dbt-adapters monorepo:
    - dbt (dbt-core)
    - dbt_postgres
    - dbt_redshift
    - dbt_snowflake
    - dbt_bigquery
    - dbt_spark
    - dbt_athena
  - Community packages (dbt_utils, dbt_expectations, dbt_date) are included
  - Partner-maintained adapters (dbt_databricks, dbt_trino, dbt_synapse) are included
  - All project-specific macros are included

Returns:
- When return_package_names_only=False (default): List of macros with name, uniqueId, description, packageName
- When return_package_names_only=True: List of unique package names (strings)
