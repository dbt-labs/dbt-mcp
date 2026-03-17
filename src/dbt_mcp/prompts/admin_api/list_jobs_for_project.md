List all jobs for a specific dbt project.

This tool retrieves jobs from the dbt Admin API, filtered by the given project ID. Use this when working with multiple projects and you need to see jobs for a specific project.

## Parameters

- **project_id** (required): The dbt project ID to list jobs for
- **limit** (optional): Maximum number of results to return
- **offset** (optional): Number of results to skip for pagination

Returns a list of job objects with details like:
- Job ID, name, and description
- Schedule configuration
- Most recent run status
- Trigger settings

Use `list_projects_and_environments` first to discover available project IDs.
