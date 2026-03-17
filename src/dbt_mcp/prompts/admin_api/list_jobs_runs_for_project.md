List job runs for a specific dbt project.

This tool retrieves runs from the dbt Admin API, filtered by the given project ID. Use this when working with multiple projects and you need to see runs for a specific project.

## Parameters

- **project_id** (required): The dbt project ID to list runs for
- **job_id** (optional, integer): Filter runs by specific job ID
- **status** (optional, string): Filter runs by status. One of: `queued`, `starting`, `running`, `success`, `error`, `cancelled`
- **limit** (optional, integer): Maximum number of results to return

Returns a list of run objects with details like:
- Run ID and status
- Job information
- Start and end times
- Git branch and SHA

Use `list_projects_and_environments` first to discover available project IDs.
