Get focused error information for a failed dbt job run.

This tool retrieves and analyzes job run failures to provide concise, actionable error details optimized for troubleshooting. Instead of verbose run details, it returns structured error information with minimal token usage.

## Parameters

- run_id (required): The run ID to analyze for error information

## Returns

Structured error information including:

- errors: List of specific error details with unique_id, relation_name, and error message
- step_name: The failed step that caused the run to fail
- finished_at: Timestamp when the failed step completed
- target: The dbt target environment where the failure occurred

## Error Types Handled

- Model execution errors
- Test failures
- Source freshness errors

## Use Cases

- Quick failure diagnosis
- LLM-optimized troubleshooting
- Automated monitoring
- Failure pattern analysis
- Rapid incident response

## Advantages over get_job_run_details

- Reduced token usage
- Structured format
- Smart filtering
- Source freshness handling

## Example Usage

```json
{
  "run_id": 789
}
```

## Example Response

```json
{
  "target": "prod",
  "step_name": "Invoke dbt with `dbt run --models staging`",
  "finished_at": "2025-09-17 14:32:15.123456+00:00",
  "errors": [
    {
      "unique_id": "model.analytics.stg_users",
      "relation_name": "analytics_staging.stg_users",
      "message": "Syntax error: Expected end of input but got keyword SELECT at line 15"
    }
  ]
}
```

## Response Information

The focused response provides only the essential error context needed for quick diagnosis and resolution of dbt job failures.