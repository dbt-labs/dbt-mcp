# Get Project Details

Get detailed information for a specific dbt Cloud project.

This tool retrieves comprehensive project information including associated environments, repositories, and configurations.

## Parameters

- **account_id** (required): The dbt Cloud account ID
- **project_id** (required): The project ID to retrieve details for

## Returns

Project object with detailed information including:

- Project metadata (ID, name, description, type)
- Connection and repository associations
- dbt project subdirectory configuration
- Related environments and their configurations
- Associated jobs for docs and freshness
- Group permissions and access controls
- Semantic layer configuration (if enabled)

## Use Cases

- Get complete project configuration for troubleshooting
- Understand project relationships and dependencies
- Audit project settings and permissions
- Retrieve connection information for debugging
- Check environment and job associations

## Example Response

```json
{
  "id": 456,
  "name": "Analytics Project",
  "description": "Main analytics dbt project",
  "connection_id": 789,
  "repository_id": 101,
  "environments": [...],
  "group_permissions": [...]
}
```
