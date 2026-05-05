List all job runs in a dbt platform account with optional filtering.

This tool retrieves runs from the dbt Admin API. Runs represent executions of dbt jobs in dbt.

Returns a list of run objects with details like:

- Run ID and status
- Job information
- Start and end times
- Git branch and SHA
- Artifacts and logs information

Use this tool to monitor job execution, check run history, or find specific runs for debugging.
