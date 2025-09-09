Retry a failed dbt job run from the point of failure.

This tool allows you to restart a failed run, continuing from where it failed rather than starting completely over.

## Parameters

- **run_id** (required): The run ID to retry

## Returns

New run object for the retry attempt with a new run ID and execution details.

## Requirements for Retry

- Original run must have failed (status 20)
- Run must be the most recent run for the job
- dbt version must support retry functionality
- Run must have generated run_results.json

## Retry Behavior

- Continues from the failed step
- Skips successfully completed models
- Uses the same configuration as the original run
- Creates a new run with a new run ID
- Maintains the same Git SHA and branch

## Use Cases

- Recover from transient infrastructure failures
- Continue after warehouse connectivity issues
- Resume after temporary resource constraints
- Avoid re-running expensive successful steps
- Quick recovery from known, fixable issues

## Retry Not Supported Reasons

If retry fails, possible reasons include:
- **RETRY_UNSUPPORTED_CMD**: Command type doesn't support retry
- **RETRY_UNSUPPORTED_VERSION**: dbt version too old
- **RETRY_NOT_LATEST_RUN**: Not the most recent run for the job
- **RETRY_NOT_FAILED_RUN**: Run didn't fail
- **RETRY_NO_RUN_RESULTS**: Missing run results for retry logic

## Example Usage

```json
{
  "run_id": 789
}
```

This creates a new run that continues from the failure point of run 789.
