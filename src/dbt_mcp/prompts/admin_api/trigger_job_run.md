# Trigger Job Run

Trigger a dbt job run with optional parameter overrides.

This tool starts a new run for a specified job with the ability to override default settings like Git branch, schema, or other execution parameters.

## Parameters

- **job_id** (required): The job ID to trigger
- **cause** (required): Description of why the job is being triggered
- **git_branch** (optional): Override the Git branch to checkout
- **git_sha** (optional): Override the Git SHA to checkout
- **schema_override** (optional): Override the destination schema

## Additional Override Options

The API supports additional overrides (can be added to the implementation):

- **dbt_version_override**: Override the dbt version
- **threads_override**: Override the number of threads
- **target_name_override**: Override the target name
- **generate_docs_override**: Override docs generation setting
- **timeout_seconds_override**: Override the timeout
- **steps_override**: Override the dbt commands to execute

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
