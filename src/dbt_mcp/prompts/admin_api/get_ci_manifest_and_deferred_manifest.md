Download and retrieve manifest.json files for CI job analysis.

This tool downloads the manifest.json files from both the current CI run and its deferred state (if available), providing the foundation for state-based analysis and model comparison.

## Parameters

- **run_id** (required): The run ID to download manifests for

## Returns

Structured response containing manifest information:

- **current_manifest**: The parsed manifest.json from the current run
- **deferred_manifest**: The parsed manifest.json from the deferred run (if available)
- **deferring_run_id**: The run ID used for deferred state (null if not available)
- **current_run_id**: The current run ID
- **manifest_comparison_available**: Boolean indicating if both manifests are available for comparison
- **total_models_current**: Number of models in current manifest
- **total_models_deferred**: Number of models in deferred manifest (if available)
- **download_paths**: Local paths where manifests were downloaded

## Manifest Contents

Each manifest contains:
- **nodes**: All dbt nodes (models, tests, seeds, snapshots, etc.)
- **sources**: Source definitions
- **macros**: Macro definitions
- **docs**: Documentation blocks
- **exposures**: Exposure definitions
- **metrics**: Metric definitions (if applicable)
- **metadata**: Run metadata and dbt version information

## Deferred State Detection

The tool automatically detects deferred state by checking:
- Job run details for `deferring_run_id` or `defer_run_id` fields
- Run results args for defer configuration
- Multiple possible locations for deferring information

## Use Cases

- **State Comparison Setup**: Prepare manifests for detailed model comparison
- **CI Analysis Foundation**: Get base data for understanding CI job behavior
- **Model Lineage Analysis**: Access complete project structure and dependencies
- **Change Detection**: Identify what models, tests, or other nodes have changed
- **Dependency Mapping**: Understand model relationships and dependencies
- **Project Structure Analysis**: Examine overall project organization

## CI Job Context

This tool is particularly valuable for CI jobs that use:
- State comparison (`--defer` flag)
- Selective execution based on changes
- Slim CI patterns
- Production state deferring

## File Management

- Downloads manifests to temporary directory
- Handles large manifest files efficiently
- Cleans up temporary files automatically
- Provides download paths for further processing

## Error Handling

- Gracefully handles missing deferred state
- Continues if deferred manifest cannot be downloaded
- Provides clear indication of what data is available
- Logs warnings for missing artifacts

## Example Usage

```json
{
  "run_id": 12345
}
```

## Example Response

```json
{
  "current_manifest": {
    "nodes": {
      "model.project.customer_orders": {
        "name": "customer_orders",
        "checksum": {"checksum": "abc123"},
        "depends_on": {"nodes": ["source.project.raw_orders"]}
      }
    }
  },
  "deferred_manifest": {
    "nodes": {
      "model.project.customer_orders": {
        "name": "customer_orders", 
        "checksum": {"checksum": "def456"},
        "depends_on": {"nodes": ["source.project.raw_orders"]}
      }
    }
  },
  "deferring_run_id": 11111,
  "current_run_id": 12345,
  "manifest_comparison_available": true,
  "total_models_current": 25,
  "total_models_deferred": 23,
  "download_paths": {
    "current_manifest": "/tmp/run_12345_manifest.json",
    "deferred_manifest": "/tmp/deferred_11111_manifest.json"
  }
}
```

## Integration

This tool is designed to work with:
- `compare_manifests` - for detailed comparison analysis
- `get_model_execution_reasons` - for execution reason analysis
- Other admin API tools for comprehensive CI analysis

## Performance

- Streams large manifest files efficiently
- Minimal memory usage during download
- Parallel download of current and deferred manifests when possible
- Optimized JSON parsing for large projects
