# Get Run Details

Get detailed information for a specific dbt Cloud run.

This tool retrieves comprehensive run information including execution details, steps, artifacts, and debug logs.

## Parameters

- **account_id** (required): The dbt platform account ID
- **run_id** (required): The run ID to retrieve details for
- **include_related** (optional): Comma-separated list of related objects to include
  - Valid values: `trigger`, `job`, `environment`, `repository`, `run_steps`, `run_retries`, `used_repo_cache`, `repo_cache_restore`, `audit`, `debug_logs`

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

## Run Statuses

- **1**: Queued - Run is waiting to start
- **2**: Starting - Run is initializing
- **3**: Running - Run is executing
- **10**: Success - Run completed successfully
- **20**: Error - Run failed with errors
- **30**: Cancelled - Run was cancelled

## Use Cases

- Monitor run progress and status
- Debug failed runs with detailed logs
- Review run performance and timing
- Check artifact generation status
- Audit run execution details
- Troubleshoot run failures

## Example Usage

```python
# Basic run details
get_run(account_id=123, run_id=789)

# Include debug logs and steps
get_run(
    account_id=123, 
    run_id=789, 
    include_related="run_steps,debug_logs,job"
)
```

## Response Information

The detailed response includes timing, status, and execution context to help with monitoring and debugging dbt Cloud runs.
