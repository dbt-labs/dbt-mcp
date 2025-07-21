# Get Job Details

Get detailed information for a specific dbt Cloud job.

This tool retrieves comprehensive job configuration including execution settings, triggers, and scheduling information.

## Parameters

- **account_id** (required): The dbt platform account ID
- **job_id** (required): The job ID to retrieve details for
- **include_related** (optional): Comma-separated list of related objects to include
  - Valid values: `environment`, `custom_environment_variables`, `most_recent_run`, `most_recent_completed_run`

## Returns

Job object with detailed configuration including:

- Job metadata (ID, name, description, type)
- Environment and project associations
- Execute steps (dbt commands to run)
- Trigger configuration (schedule, webhooks, CI)
- Execution settings (timeout, threads, target)
- dbt version overrides
- Generate docs and sources settings
- Most recent run information (if requested)

## Use Cases

- Debug job configuration issues
- Understand job execution settings
- Review scheduling and trigger configuration
- Check dbt commands and execution steps
- Audit job settings across projects
- Get recent run status for monitoring

## Job Types

- **ci**: Continuous integration jobs
- **scheduled**: Regularly scheduled jobs
- **other**: Manual or API-triggered jobs
- **merge**: Jobs triggered on merge events

## Example Usage

```python
# Basic job details
get_job(account_id=123, job_id=456)

# Include recent run information
get_job(account_id=123, job_id=456, include_related="most_recent_run,environment")
```
