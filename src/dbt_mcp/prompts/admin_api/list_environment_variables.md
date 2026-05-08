List all environment variables for a specific project in the dbt Cloud account.

This tool retrieves environment variables from the dbt Admin API. Environment variables in dbt Cloud are used to customize project behavior across different environments (development, staging, production) without changing code.

Parameters:
- **project_id**: The ID of the project to list environment variables for

Returns a mapping of environment variable names to objects containing:
- **project**: The value set at the project level
- **environments**: Per-environment override values (for example, development, staging, production)

Use this tool to inspect how environment variables are configured across environments, debug environment-specific behavior, or understand project configuration.
