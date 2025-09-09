Download a specific artifact file from a dbt job run.

This tool retrieves the content of a specific artifact file generated during run execution, such as manifest.json, catalog.json, or compiled SQL files.

## Parameters

- **run_id** (required): The run ID containing the artifact
- **artifact_path** (required): The path to the specific artifact file
- **step** (optional): The step index to retrieve artifacts from (default: last step)

## Common Artifact Paths

- **manifest.json**: Complete dbt project metadata, models, and lineage
- **catalog.json**: Table and column documentation with statistics
- **run_results.json**: Execution results, timing, and status information
- **sources.json**: Source freshness check results
- **compiled/[model_path].sql**: Individual compiled SQL files
- **logs/dbt.log**: Complete execution logs

## Returns

The artifact content in its original format:
- JSON files return parsed JSON objects
- SQL files return text content
- Log files return text content

## Use Cases

- Download manifest.json for lineage analysis
- Get catalog.json for documentation systems
- Retrieve run_results.json for execution monitoring
- Access compiled SQL for debugging
- Download logs for troubleshooting failures
- Integration with external tools and systems

## Step Selection

- By default, artifacts from the last step are returned
- Use the `step` parameter to get artifacts from earlier steps
- Step indexing starts at 1 for the first step

## Example Usage

```json
{
  "run_id": 789,
  "artifact_path": "manifest.json"
}
```

```json
{
  "run_id": 789,
  "artifact_path": "compiled/analytics/models/staging/stg_users.sql"
}
```

```json
{
  "run_id": 789,
  "artifact_path": "run_results.json",
  "step": 2
}
```
