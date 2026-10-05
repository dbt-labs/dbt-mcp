List all jobs in a dbt platform account with optional filtering.

This tool retrieves jobs from the dbt Admin API. Jobs are the configuration for scheduled or triggered dbt runs.

Pass `project_id` to list jobs across all environments in that project. This overrides the configured production environment filter. When `project_id` is omitted, results remain scoped to the configured production environment, or to the account if no environment is configured. Use `limit` and `offset` to page through large result sets.

Returns a list of job objects with details like:
- Job ID, name, and description
- Environment ID and project ID the job belongs to
- Schedule configuration
- Execute steps (dbt commands)
- Trigger settings

Use this tool to explore available jobs, understand job configurations, or find specific jobs to trigger.
