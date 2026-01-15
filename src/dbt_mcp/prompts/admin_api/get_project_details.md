Get detailed information for a specific dbt Cloud project.

This tool retrieves comprehensive project configuration including repository connections, environment settings, and project metadata.

## Parameters

- **project_id** (required): The project ID to retrieve details for

## Returns

Project object with detailed configuration including:

- Project metadata (ID, name, description)
- Account association
- Repository connection details
- Default environment and job configurations
- Project settings and feature flags
- Creation and update timestamps

## Use Cases

- Review project configuration and settings
- Check repository connection details
- Understand project-environment relationships
- Audit project settings across accounts
- Debug project setup issues
- Retrieve project metadata for automation

## Example Usage

```json
{
  "project_id": 123
}
```
