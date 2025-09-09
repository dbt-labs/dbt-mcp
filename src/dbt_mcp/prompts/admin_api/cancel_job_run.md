Cancel a currently running or queued dbt run.

This tool allows you to stop a run that is currently executing or waiting in the queue.

## Parameters

- **run_id** (required): The run ID to cancel

## Returns

Updated run object showing the cancelled status and timing information.

## Run States That Can Be Cancelled

- **Queued (1)**: Run is waiting to start
- **Starting (2)**: Run is initializing
- **Running (3)**: Run is currently executing

## Use Cases

- Stop long-running jobs that are no longer needed
- Cancel jobs that were triggered by mistake
- Free up run slots for higher priority jobs
- Stop runs that are stuck or hanging
- Emergency cancellation during incidents

## Important Notes

- Once cancelled, a run cannot be resumed
- Partial work may have been completed before cancellation
- Artifacts from cancelled runs may not be available
- Use the retry functionality if you need to re-run after cancellation

## Example Usage

```json
{
  "run_id": 789
}
```

## Response

Returns the updated run object with:
- Status changed to cancelled (30)
- Cancellation timestamp
- Final execution timing
- Any artifacts that were generated before cancellation
