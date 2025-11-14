Get focused error and/or warning information for a dbt job run.

This tool retrieves and analyzes job runs to provide concise, actionable error and warning details optimized for troubleshooting and monitoring. Instead of verbose run details, it returns structured information with minimal token usage.

## Parameters

- run_id (required): The run ID to analyze for error/warning information
- include_warnings (optional, default: False): If True, include warnings along with errors in the response
- warning_only (optional, default: False): If True, only return warnings without errors (useful for analyzing successful runs)

## Returns

### Default Behavior (errors only)
Structured error information with `failed_steps` containing a list of failed step details:

- failed_steps: List of failed steps, each containing:
  - target: The dbt target environment where the failure occurred
  - step_name: The failed step that caused the run to fail
  - finished_at: Timestamp when the failed step completed
  - results: List of specific error details, each with:
    - unique_id: Model/test unique identifier (nullable)
    - relation_name: Database relation name or "No database relation"
    - message: Error message
    - compiled_code: Raw compiled SQL code (nullable)
    - truncated_logs: Raw truncated debug log output (nullable)

NOTE: The "truncated_logs" key only populates if there is no `run_results.json` artifact to parse after a job run error.

### When include_warnings=True
Returns both error information (as above) plus a `warnings` object containing:

- has_warnings: Boolean indicating if any warnings were found
- warning_steps: List of successful steps containing warnings
- log_warnings: List of warnings extracted from logs
- summary: Aggregate warning counts

### When warning_only=True
Returns only warning information (same structure as the `warnings` object above):

- has_warnings: Boolean indicating if any warnings were found
- warning_steps: List of successful steps containing warnings, each with:
  - target: The dbt target environment
  - step_name: The step that generated warnings
  - finished_at: Timestamp when the step completed
  - results: List of specific warning details, each with:
    - unique_id: Model/test/source unique identifier
    - relation_name: Database relation name or source name
    - message: Warning message describing the issue
    - status: Always "warn" to distinguish from errors
    - compiled_code: Raw compiled SQL code (optional, for test warnings)
- log_warnings: List of warnings extracted directly from logs (no unique_id)
- summary: Aggregate warning counts:
  - total_warnings: Total number of warnings found
  - test_warnings: Number of test warnings (tests with `severity: warn`)
  - freshness_warnings: Number of source freshness warnings
  - log_warnings: Number of warnings extracted from logs

## Example Usage

### Get errors only (default)
```json
{
  "run_id": 789
}
```

### Get errors with warnings
```json
{
  "run_id": 789,
  "include_warnings": true
}
```

### Get warnings only
```json
{
  "run_id": 789,
  "warning_only": true
}
```