List all available artifacts for a completed dbt job run.

This tool retrieves the list of artifact files generated during a run execution, such as manifest.json, catalog.json, and run_results.json.

## Parameters

- **run_id** (required): The run ID to list artifacts for

## Returns

List of artifact file paths available for download. Common artifacts include:

- **manifest.json**: Complete project metadata and lineage
- **catalog.json**: Documentation and column information
- **run_results.json**: Execution results and timing
- **sources.json**: Source freshness check results
- **compiled/**: Compiled SQL files
- **run/**: SQL statements executed during the run

## Artifact Availability

Artifacts are only available for:
- Successfully completed runs
- Failed runs that progressed beyond compilation
- Runs where `artifacts_saved` is true

## Use Cases

- Discover available artifacts before downloading
- Check if specific artifacts were generated
- Audit artifact generation across runs
- Integrate with external systems that consume dbt artifacts
- Validate run completion and output generation

## Example Usage

```json
{
  "run_id": 789
}
```

## Next Steps

Use `get_run_artifact` to download specific artifacts from this list for analysis or integration with other tools.
