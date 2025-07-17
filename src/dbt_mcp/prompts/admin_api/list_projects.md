# List Projects

List all projects in a dbt Cloud account with optional filtering.

This tool retrieves projects from the dbt Cloud Admin API v2. Projects are the top-level containers for dbt code and configurations.

## Parameters

- **account_id** (required): The dbt Cloud account ID
- **project_id** (optional): Filter to a specific project by ID
- **limit** (optional): Maximum number of results to return (default: 100)
- **offset** (optional): Number of results to skip for pagination

## Returns

List of project objects with details including:

- Project ID, name, and description
- Associated connection and repository information
- dbt project subdirectory path
- Semantic layer configuration
- Environment and job relationships
- Created/updated timestamps

## Use Cases

- Discover available projects in an account
- Find project IDs for other API operations
- Audit project configurations across an account
- Implement project-based access controls

## Example Usage

- List all projects: Provide only `account_id`
- Get specific project: Include `project_id` parameter
- Paginate results: Use `limit` and `offset` parameters
