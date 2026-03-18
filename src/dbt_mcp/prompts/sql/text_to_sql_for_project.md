Generate SQL from a natural language query for a specific dbt project.

This tool translates natural language into SQL using the context of the specified project's production environment. Use this when working with multiple projects.

## Parameters

- **project_id** (required): The dbt project ID to generate SQL for. The project must have a production environment configured.
- **query** (required): A natural language description of the data you want to query.

Returns the generated SQL query.

Use `list_projects_and_environments` first to discover available project IDs.
