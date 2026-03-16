List all active projects in the dbt Cloud account.

This tool retrieves projects from the dbt Admin API. Projects are the top-level organizational unit in dbt Cloud, each associated with a git repository containing dbt code.

Returns a list of project objects with:
- **id**: The project ID
- **name**: The project name
- **description**: The project description (if set)

Use this tool to discover available projects in the account, especially when working across multiple projects.
