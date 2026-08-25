# Trigger Job Run

Trigger a dbt job run with optional parameter overrides.

This tool starts a new run for a specified job with the ability to override default settings like Git branch, schema, or other execution parameters.

## Returns

Run object with information about the newly triggered run including:

- Run ID and status
- Job and environment information
- Git branch and SHA being used
- Trigger information and cause
- Execution queue position

## Use Cases

- Trigger ad-hoc job runs for testing
- Run jobs with different Git branches for feature testing
- Execute jobs with schema overrides for development
- Trigger jobs via API automation or external systems
- Run jobs with custom parameters for specific scenarios
- Verify a project builds on a newer dbt version before switching the environment over to it

## Example Usage

```json
{
  "job_id": 456,
  "cause": "Manual trigger for testing"
}
```

```json
{
  "job_id": 456,
  "cause": "Testing feature branch",
  "git_branch": "feature/new-models",
  "schema_override": "dev_testing"
}
```

```json
{
  "job_id": 456,
  "cause": "Selective production build",
  "steps_override": ["dbt run --select my_model+ --full-refresh"]
}
```

```json
{
  "job_id": 456,
  "cause": "Verifying the upgrade",
  "git_branch": "upgrade-dbt-core",
  "dbt_version_override": "latest",
  "schema_override": "dbt_upgrade_verify"
}
```
