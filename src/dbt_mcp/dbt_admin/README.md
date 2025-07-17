# dbt Cloud Admin API v2 Integration

This directory contains the implementation for integrating with the dbt Cloud Admin API v2 instead of the semantic layer endpoints.

## Overview

The Admin API v2 provides access to dbt Cloud administrative functionality including:
- Account management
- Project and environment management
- Job configuration and execution
- Run monitoring and control
- User and permission management
- Artifact retrieval

## Architecture

### Client (`client.py`)
- `DbtAdminAPIClient`: Main client class for interacting with the Admin API v2
- Uses `requests` library for HTTP communication
- Implements caching with `@cache` decorator where appropriate
- Handles authentication via Bearer tokens
- Provides methods for all major API endpoints

### Tools (`tools.py`)
- `register_admin_api_tools()`: Registers all admin API tools with the MCP server
- Each tool corresponds to a specific API endpoint
- Comprehensive error handling and logging
- Optional parameter support for filtering and pagination

## Key Features

### Account Management
- List all accessible accounts
- Get account details and settings

### Project & Environment Management
- List and retrieve projects
- List and retrieve environments
- Filter by various criteria

### Job Management
- List jobs with filtering options
- Get detailed job configurations
- Trigger job runs with custom parameters
- Support for schema overrides, git branches, etc.

### Run Management
- List runs with extensive filtering
- Get detailed run information including steps and logs
- Cancel and retry runs
- Retrieve run artifacts (manifest.json, catalog.json, etc.)

### User Management
- List account users
- Get user details and permissions

## API Endpoints Supported

Based on the dbt Cloud Admin API v2 OpenAPI specification:

- `GET /api/v2/accounts/` - List accounts
- `GET /api/v2/accounts/{account_id}/` - Get account details
- `GET /api/v2/accounts/{account_id}/projects/` - List projects
- `GET /api/v2/accounts/{account_id}/environments/` - List environments
- `GET /api/v2/accounts/{account_id}/jobs/` - List jobs
- `POST /api/v2/accounts/{account_id}/jobs/{job_id}/run/` - Trigger job run
- `GET /api/v2/accounts/{account_id}/runs/` - List runs
- `GET /api/v2/accounts/{account_id}/runs/{run_id}/` - Get run details
- `POST /api/v2/accounts/{account_id}/runs/{run_id}/cancel/` - Cancel run
- `POST /api/v2/accounts/{account_id}/runs/{run_id}/retry/` - Retry run
- `GET /api/v2/accounts/{account_id}/runs/{run_id}/artifacts/` - List artifacts
- `GET /api/v2/accounts/{account_id}/runs/{run_id}/artifacts/{path}` - Get artifact
- `GET /api/v2/accounts/{account_id}/users/` - List users

## Configuration

Uses the existing `RemoteConfig` configuration class:

```python
@dataclass
class RemoteConfig:
    multicell_account_prefix: str | None
    host: str
    user_id: int
    dev_environment_id: int
    prod_environment_id: int
    token: str
```

## Error Handling

- Custom `AdminAPIError` exception for API-specific errors
- Comprehensive logging for debugging
- Graceful error responses returned as strings to MCP clients

## Usage Examples

### List Accounts
```python
accounts = admin_client.list_accounts()
```

### Trigger a Job Run
```python
run = admin_client.trigger_job_run(
    account_id=123,
    job_id=456,
    cause="Manual trigger via MCP",
    git_branch="feature/new-models"
)
```

### Get Run Artifacts
```python
artifacts = admin_client.list_run_artifacts(account_id=123, run_id=789)
manifest = admin_client.get_run_artifact(
    account_id=123, 
    run_id=789, 
    artifact_path="manifest.json"
)
```

## Integration

The admin API tools are automatically registered when a `remote_config` is available:

```python
if config.remote_config:
    logger.info("Registering remote tools")
    await register_remote_tools(dbt_mcp, config.remote_config)
    
    logger.info("Registering admin API tools")
    register_admin_api_tools(dbt_mcp, config.remote_config)
```

## Migration from Semantic Layer

This implementation replaces the semantic layer client functionality:

### Before (Semantic Layer)
- GraphQL queries for metrics, dimensions, entities
- Semantic layer specific authentication
- Limited to semantic layer operations

### After (Admin API v2)
- RESTful API for full dbt Cloud administration
- Token-based authentication
- Comprehensive dbt Cloud management capabilities

## Future Enhancements

- Add support for v3 API endpoints where available
- Implement webhook management
- Add batch operations for efficiency
- Enhanced filtering and search capabilities
- Real-time run monitoring via streaming APIs
