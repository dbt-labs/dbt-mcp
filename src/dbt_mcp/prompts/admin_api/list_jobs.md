List all jobs in a dbt platform account with optional filtering.

This tool retrieves jobs from the dbt Admin API. Jobs are the configuration for scheduled or triggered dbt runs.

## Parameters

- **project_id** (optional): Filter jobs by specific project ID
- **environment_id** (optional): Filter jobs by specific environment ID
- **limit** (optional): Maximum number of results to return
- **offset** (optional): Number of results to skip for pagination

Returns a list of job objects with details like:
- Job ID, name, and description
- Environment and project information
- Schedule configuration
- Execute steps (dbt commands)
- Trigger settings

Use this tool to explore available jobs, understand job configurations, or find specific jobs to trigger.
