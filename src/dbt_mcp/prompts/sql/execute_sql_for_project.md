Execute SQL against a specific dbt project's development environment.

This tool runs SQL queries on the dbt Platform infrastructure for the specified project's dev environment. Use this when working with multiple projects.

## Parameters

- **project_id** (required): The dbt project ID to execute SQL against. The project must have a development environment configured.
- **sql_query** (required): The SQL query to execute.
- **limit** (optional, integer): Maximum number of rows to return.

Returns the query results.

Use `list_projects_and_environments` first to discover available project IDs.
