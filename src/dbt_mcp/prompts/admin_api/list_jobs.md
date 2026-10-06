List jobs in the selected project's production environment.

Jobs configure scheduled or triggered dbt runs. When a project selector is exposed, use `project_id` to select the project; otherwise the current context already selects it. A production environment is required. Use `limit` and `offset` to page through large result sets.

The `result` field contains a list of job objects with details like:
- Job ID, name, and description
- Environment ID and project ID the job belongs to
- Schedule configuration
- Execute steps (dbt commands)
- Trigger settings

Use this tool to explore available jobs, understand job configurations, or find specific jobs to trigger.

Returns one page in `result`, with `pagination.has_more` and `pagination.next_offset`. Use the next offset with the same filters. `limit` defaults to 50 and is capped at 100.
