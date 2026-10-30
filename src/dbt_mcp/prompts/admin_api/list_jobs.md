List jobs in a project, a configured environment, or across the account.

Jobs configure scheduled or triggered dbt runs. When `project_id` is exposed, it is optional: provide it to list jobs across all environments in that project, overriding the configured environment filter. When omitted, use the configured environment (including staging), then the configured project if there is no environment. If neither is configured, list jobs across the account. A bound project selector is hidden and the configured context supplies the default. Use `limit` and `offset` to page through large result sets.

The `result` field contains a list of job objects with details like:
- Job ID, name, and description
- Environment ID and project ID the job belongs to
- Schedule configuration
- Execute steps (dbt commands)
- Trigger settings

Use this tool to explore available jobs, understand job configurations, or find specific jobs to trigger.

Returns one page in `result`, with `pagination.has_more` and `pagination.next_offset`. Use the next offset with the same filters. `limit` defaults to 50 and is capped at 100.
