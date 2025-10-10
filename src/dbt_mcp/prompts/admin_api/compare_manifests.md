Compare two dbt manifest.json files to identify changes between states.

This tool performs detailed comparison between current and deferred manifest files to identify what models, tests, and other nodes have changed, providing insights for CI job analysis and state-based execution. It reads manifest files from local file paths and performs the comparison analysis.

## Parameters

- **current_manifest_path** (required): File path to the current manifest.json file
- **deferred_manifest_path** (required): File path to the deferred manifest.json file
- **comparison_scope** (optional): Scope of comparison - "models", "tests", "all" (default: "models")

## Returns

Detailed comparison analysis including:

- **comparison_summary**: High-level summary of changes detected
- **changed_models**: List of models that have changed, each containing:
  - unique_id: Model unique identifier
  - name: Model name
  - change_type: Type of change (new, modified, removed, dependencies_changed)
  - change_details: Specific details about what changed
  - checksum_changed: Boolean indicating if model code changed
  - dependencies_changed: Boolean indicating if dependencies changed
  - materialization_changed: Boolean indicating if materialization changed
- **unchanged_models**: List of models that are identical between states
- **new_models**: List of models present in current but not deferred state
- **removed_models**: List of models present in deferred but not current state
- **dependency_changes**: Analysis of dependency relationship changes
- **metadata_changes**: Changes in project metadata, dbt version, etc.

## Change Types

**Model Changes Detected**:
- **new**: Model exists in current but not deferred manifest
- **removed**: Model exists in deferred but not current manifest  
- **modified**: Model code changed (checksum difference)
- **dependencies_changed**: Model dependencies added, removed, or modified
- **config_changed**: Model configuration changed (materialization, etc.)
- **description_changed**: Model description or documentation changed

**Dependency Analysis**:
- Added dependencies
- Removed dependencies  
- Changed dependency types
- Circular dependency detection
- Upstream/downstream impact analysis

## Comparison Scope Options

- **"models"**: Compare only dbt models (default)
- **"tests"**: Compare only tests
- **"all"**: Compare models, tests, sources, macros, and other nodes

## Use Cases

- **CI Job Analysis**: Understand what triggered model execution
- **Change Impact Assessment**: See downstream effects of changes
- **Code Review**: Identify all changes between branches/states
- **Deployment Planning**: Understand scope of changes before deployment
- **Performance Analysis**: Identify models that may need optimization
- **Dependency Auditing**: Track how model relationships evolve

## Change Detection Methods

**Checksum Comparison**:
- Compares model file checksums to detect code changes
- Identifies SQL modifications, config changes
- Detects changes in model definitions

**Dependency Analysis**:
- Compares `depends_on.nodes` arrays
- Identifies added/removed upstream dependencies
- Detects changes in dependency types

**Configuration Comparison**:
- Materialization changes (table → view, etc.)
- Configuration parameter changes
- Tag and meta changes

## Example Usage

```json
{
  "current_manifest_path": "/tmp/run_12345_manifest.json",
  "deferred_manifest_path": "/tmp/deferred_11111_manifest.json",
  "comparison_scope": "models"
}
```

## Example Response

```json
{
  "comparison_summary": "Found 3 changed models, 1 new model, 0 removed models out of 25 total models",
  "changed_models": [
    {
      "unique_id": "model.project.customer_orders",
      "name": "customer_orders",
      "change_type": "modified",
      "change_details": "Model code changed and dependencies modified",
      "checksum_changed": true,
      "dependencies_changed": true,
      "materialization_changed": false,
      "current_checksum": "abc123",
      "deferred_checksum": "def456",
      "added_dependencies": ["model.project.customers"],
      "removed_dependencies": []
    }
  ],
  "unchanged_models": [
    {
      "unique_id": "model.project.customer_summary",
      "name": "customer_summary",
      "checksum": "xyz789"
    }
  ],
  "new_models": [
    {
      "unique_id": "model.project.new_analysis",
      "name": "new_analysis"
    }
  ],
  "removed_models": [],
  "dependency_changes": {
    "total_dependency_changes": 1,
    "models_with_new_dependencies": ["model.project.customer_orders"],
    "models_with_removed_dependencies": []
  },
  "metadata_changes": {
    "dbt_version_changed": false,
    "project_name_changed": false
  }
}
```

## Performance Considerations

- Efficient comparison algorithms for large manifests
- Optimized for projects with hundreds of models
- Memory-efficient processing of large JSON structures
- Fast checksum and dependency comparison

## Integration

This tool works seamlessly with:
- `get_ci_manifest_and_deferred_manifest` - provides input manifest file paths via `download_paths`
- Other CI analysis tools for comprehensive understanding

## Workflow Integration

Typical workflow:
1. Use `get_ci_manifest_and_deferred_manifest` to download manifest files
2. Extract `current_manifest` and `deferred_manifest` paths from `download_paths` in the response
3. Use `compare_manifests` with these file paths to analyze differences

## Error Handling

- Validates manifest structure before comparison
- Handles missing or malformed manifest sections
- Provides clear error messages for invalid inputs
- Graceful handling of version differences between manifests
