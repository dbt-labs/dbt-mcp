List all dbt Cloud projects and their environments that you have access to.

Call this tool first to discover available project IDs before calling any other project-scoped tools.

Returns a list of project entries, each containing:
- **project_id**: The project ID (use this when calling other tools)
- **project_name**: The project name
- **account_id**: The account ID the project belongs to
- **account_name**: The account name
- **prod_environment_id**: The production environment ID (null if none found)
- **prod_environment_name**: The production environment name (null if none found)
- **dev_environment_id**: The development environment ID (null if none found)
- **dev_environment_name**: The development environment name (null if none found)
