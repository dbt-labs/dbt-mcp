# List Environments

List all environments in a dbt Cloud account with optional filtering.

This tool retrieves environments from the dbt Cloud Admin API v2. Environments define the target data warehouse and dbt version settings for job execution.

## Parameters

- **account_id** (required): The dbt Cloud account ID
- **project_id** (optional): Filter environments by specific project ID
- **limit** (optional): Maximum number of results to return
- **offset** (optional): Number of results to skip for pagination

## Returns

List of environment objects with details including:

- Environment ID, name, and type (development/deployment)
- Associated project and connection information
- dbt version configuration
- Git branch settings (custom branch usage)
- Deployment type (production/staging)
- Environment-specific settings and variables

## Use Cases

- Discover available environments for job configuration
- Find environment IDs for triggering runs
- Audit environment configurations across projects
- Check dbt version consistency across environments
- Identify development vs production environments

## Environment Types

- **Development**: For interactive development and testing
- **Deployment**: For scheduled jobs and production workloads

## Example Usage

- List all environments: Provide only `account_id`
- Filter by project: Include `project_id` parameter
- Paginate large results: Use `limit` and `offset`
