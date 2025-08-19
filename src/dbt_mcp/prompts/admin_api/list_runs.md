List all runs in a dbt platform account with optional filtering.

This tool retrieves runs from the dbt Cloud Admin API v2. Runs represent executions of dbt jobs in dbt Cloud.

## Parameters

- **job_id** (optional, integer): Filter runs by specific job ID
- **status** (optional, integer): Filter runs by status. 1=Queued, 2=Starting, 3=Running, 10=Success, 20=Error, 30=Cancelled
- **limit** (optional, integer): Maximum number of results to return
- **offset** (optional, integer): Number of results to skip for pagination
- **order_by** (optional, string): Field to order results by (e.g., "created_at", "finished_at", "id"). Use a `-` prefix for reverse ordering (e.g., "-created_at" for newest first)

Returns a list of run objects with details like:

- Run ID and status
- Job information
- Start and end times
- Git branch and SHA
- Artifacts and logs information

Use this tool to monitor job execution, check run history, or find specific runs for debugging.
