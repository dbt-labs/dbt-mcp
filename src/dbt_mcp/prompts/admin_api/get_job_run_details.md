Get detailed information for a specific dbt job run.

This tool retrieves comprehensive run information including execution details, steps, and artifacts.

## Parameters

- **run_id** (required): The run ID to retrieve details for

## Returns

Run object with detailed execution information including:

- Run metadata (ID, status, timing information)
- Job and environment details
- Git branch and SHA information
- Execution steps and their status
- Artifacts and logs availability
- Trigger information and cause
- Performance metrics and timing

## Use Cases

- Monitor run progress and status
- Debug failed runs with detailed logs
- Review run performance and timing
- Check artifact generation status
- Audit run execution details
- Troubleshoot run failures

## Example Usage

```json
{
  "run_id": 789
}
```

```json
{
  "run_id": 789
}
```

## Response Information

The detailed response includes timing, status, and execution context to help with monitoring and debugging dbt job runs.
