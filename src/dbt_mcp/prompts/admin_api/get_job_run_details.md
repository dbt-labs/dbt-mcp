Get detailed information for a specific dbt job run.

This tool retrieves comprehensive run information including execution details, steps, artifacts, and debug logs.

## Parameters

- **run_id** (required): The run ID to retrieve details for
- **debug** (optional): Set to True only if the person is explicitely asking for debug level logs. Otherwise, do not set if just the logs are asked.

## Returns

Run object with detailed execution information including:

- Run metadata (ID, status, timing information)
- Job and environment details
- Git branch and SHA information
- Execution steps and their status
- Artifacts and logs availability
- Trigger information and cause
- Debug logs (if requested)
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
  "run_id": 789,
  "debug": true
}
```

## Debug Logs

When the `debug` parameter is set to true, the response will contain detailed debug logs that can help troubleshoot run failures.

## Response Information

The detailed response includes timing, status, and execution context to help with monitoring and debugging dbt job runs.
