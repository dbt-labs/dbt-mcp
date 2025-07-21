List all runs in a dbt platform account with optional filtering.

This tool retrieves runs from the dbt Cloud Admin API v2. Runs represent executions of dbt jobs in dbt Cloud.

Parameters:
- account_id (required): The dbt platform account ID
- job_id (optional): Filter runs by specific job ID
- project_id (optional): Filter runs by specific project ID
- environment_id (optional): Filter runs by specific environment ID
- status (optional): Filter runs by status (e.g., "success", "error", "cancelled")
- limit (optional): Maximum number of results to return
- offset (optional): Number of results to skip for pagination
- order_by (optional): Field to order results by (e.g., "created_at", "finished_at", "id"). Use a `-` prefix for reverse ordering (e.g., "-created_at" for newest first)

Returns a list of run objects with details like:

- Run ID and status
- Job and environment information
- Start and end times
- Git branch and SHA
- Artifacts and logs information

Use this tool to monitor job execution, check run history, or find specific runs for debugging.
