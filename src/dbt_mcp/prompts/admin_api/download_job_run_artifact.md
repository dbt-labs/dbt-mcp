Download and save a specific artifact file from a dbt job run to the local filesystem.

This tool downloads the content of a specific artifact file generated during run execution and saves it to a specified local path. Unlike `get_job_run_artifact` which returns the content, this tool writes the file to disk.

## Parameters

- **run_id** (required): The run ID containing the artifact
- **artifact_path** (required): The path to the specific artifact file
- **save_path** (required): Local filesystem path where the artifact should be saved
- **step** (optional): The step index to retrieve artifacts from (default: last step)

## Common Artifact Paths

- **manifest.json**: Complete dbt project metadata, models, and lineage
- **catalog.json**: Table and column documentation with statistics
- **run_results.json**: Execution results, timing, and status information
- **sources.json**: Source freshness check results
- **compiled/[model_path].sql**: Individual compiled SQL files
- **logs/dbt.log**: Complete execution logs

## Returns

Success confirmation with details about the downloaded file:
- **success**: Boolean indicating successful download
- **artifact_path**: The artifact path that was downloaded
- **save_path**: The local path where the file was saved
- **file_size_bytes**: Size of the downloaded file in bytes
- **content_type**: Type of content (json, sql, log, etc.)

## Use Cases

- Download artifacts for offline analysis and archival
- Save manifest.json for external lineage tools
- Archive run_results.json for historical analysis
- Download compiled SQL for code review processes
- Save logs for detailed debugging sessions
- Integration with CI/CD pipelines that need artifact files
- Backup important run artifacts locally

## File Handling

- Creates parent directories if they don't exist
- Overwrites existing files at the save path
- Preserves original file format and content
- Handles both text and binary artifacts appropriately

## Step Selection

- By default, artifacts from the last step are downloaded
- Use the `step` parameter to get artifacts from earlier steps
- Step indexing starts at 1 for the first step

## Example Usage

```json
{
  "run_id": 789,
  "artifact_path": "manifest.json",
  "save_path": "/path/to/save/manifest.json"
}
```

```json
{
  "run_id": 789,
  "artifact_path": "run_results.json",
  "save_path": "./artifacts/run_789_results.json"
}
```

```json
{
  "run_id": 789,
  "artifact_path": "compiled/analytics/models/staging/stg_users.sql",
  "save_path": "./compiled_sql/stg_users.sql",
  "step": 2
}
```

## Error Handling

- Returns error details if artifact doesn't exist
- Handles permission errors for save path
- Validates artifact path format
- Provides clear error messages for troubleshooting

## Differences from get_job_run_artifact

- **get_job_run_artifact**: Returns artifact content as text/JSON for immediate use
- **download_job_run_artifact**: Saves artifact to filesystem for persistent storage

## Integration

This tool complements other admin API tools:
- Use `list_job_run_artifacts` to discover available artifacts first
- Use `get_job_run_details` to verify run completion
- Use `get_job_run_artifact` for quick content inspection before downloading
