List all active projects in the dbt Cloud account.

This tool retrieves projects from the dbt Admin API. Projects are the top-level organizational unit in dbt Cloud, each associated with a git repository containing dbt code.

Returns a list of project objects with:
- **id**: The project ID
- **name**: The project name
- **description**: The project description (if set)
- **dbt_project_subdirectory**: Path within the repository where the dbt project lives (if set)
- **has_semantic_layer**: Whether the project has a semantic layer configured
- **type**: The project type (0=default, 1=hybrid)
- **environments**: List of environments, each with id, name, and type (development, production, staging, or generic)
- **repository_full_name**: The repository name in `org/repo` format (if set)

Use this tool to discover available projects in the account, especially when working across multiple projects.
