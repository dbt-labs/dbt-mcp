List a page of jobs in a dbt platform account with optional filtering.

This tool retrieves jobs from the dbt Admin API. Jobs are the configuration for scheduled or triggered dbt runs.

Pass `project_id` to list jobs across all environments in that project. This overrides the configured production environment filter. When `project_id` is omitted, results remain scoped to the configured production environment, or to the account if no environment is configured. Use `limit` and `offset` to page through large result sets.

The `result` field contains a list of job objects with details like:
- Job ID, name, and description
- Environment ID and project ID the job belongs to
- Schedule configuration
- Execute steps (dbt commands)
- Trigger settings

Use this tool to explore available jobs, understand job configurations, or find specific jobs to trigger.

Returns one page in `result`, with `pagination.has_more` and `pagination.next_offset`. Use the next offset with the same filters. `limit` defaults to 50 and is capped at 100.
