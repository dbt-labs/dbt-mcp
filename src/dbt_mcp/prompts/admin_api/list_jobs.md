List all jobs in a dbt platform account with optional filtering.

This tool retrieves jobs from the dbt Admin API. Jobs are the configuration for scheduled or triggered dbt runs.

## Parameters

- **limit** (optional): Maximum number of results to return
- **offset** (optional): Number of results to skip for pagination

When a single production environment is configured, results are automatically scoped to that environment. Otherwise, all jobs in the account are returned — use `limit` to constrain large result sets.

Returns a list of job objects with details like:
- Job ID, name, and description
- Environment ID and project ID the job belongs to
- Schedule configuration
- Execute steps (dbt commands)
- Trigger settings

Use this tool to explore available jobs, understand job configurations, or find specific jobs to trigger.
