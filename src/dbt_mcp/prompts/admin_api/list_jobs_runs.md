List all job runs in a dbt platform account with optional filtering.

This tool retrieves runs from the dbt Admin API. Runs represent executions of dbt jobs in dbt.

The `result` field contains a list of run objects with details like:

- Run ID and status
- Job information
- Start and end times
- Git branch and SHA
- Artifacts and logs information

Use this tool to monitor job execution, check run history, or find specific runs for debugging.

Returns one page in `result`, with `pagination.has_more` and `pagination.next_offset`. Use the next offset with the same filters. `limit` defaults to 50 and is capped at 100.
