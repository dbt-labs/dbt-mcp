# Get Environment Details

Get detailed information for a specific dbt Cloud environment.

This tool retrieves comprehensive environment configuration including connection details, custom variables, and associated jobs.

## Parameters

- **account_id** (required): The dbt Cloud account ID
- **environment_id** (required): The environment ID to retrieve details for

## Returns

Environment object with detailed information including:

- Environment metadata (ID, name, type, deployment type)
- Project and connection associations
- dbt version and project subdirectory settings
- Git configuration (custom branch settings)
- Associated repository information
- Custom environment variables
- Related jobs that use this environment
- Connection details for the data warehouse

## Use Cases

- Debug environment-specific job failures
- Understand environment configuration for troubleshooting
- Audit environment settings and variables
- Check connection and credential configurations
- Verify dbt version compatibility
- Review custom branch and Git settings

## Environment Information

The response includes:

- **Connection**: Data warehouse connection details
- **Repository**: Git repository configuration
- **Jobs**: Associated jobs using this environment
- **Variables**: Custom environment variables
- **Settings**: dbt version, branch, and path configurations

## Example Response

```json
{
  "id": 789,
  "name": "Production",
  "type": "deployment",
  "deployment_type": "production",
  "dbt_version": "1.7.0",
  "connection": {...},
  "custom_environment_variables": [...]
}
```
