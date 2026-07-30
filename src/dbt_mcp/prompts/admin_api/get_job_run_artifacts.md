Get a specific artifact file from a dbt job run.

This tool retrieves the content of a specific artifact file generated during run execution, such as manifest.json, catalog.json, or run_results.json.

## Common Artifact Paths

- **manifest.json**: Complete dbt project metadata, models, and lineage
- **catalog.json**: Table and column documentation with statistics
- **run_results.json**: Execution results, timing, and status information
- **sources.json**: Source freshness check results
- **logs/dbt.log**: Complete execution logs

Use `list_job_run_artifacts` first to see which artifacts are available for a run.

## Output Options

- **Default (no output_path, no jq_filter)**: Returns artifact content as a string for artifacts under 500 KB. For artifacts 500 KB and above, the tool automatically writes to a temp file and returns the path — no extra parameters needed. This covers the full range from small `run_results.json` files up to `manifest.json` on projects with thousands of models.
- **jq_filter set**: Applies a jq filter to the artifact and returns the matching results as a **JSON array** inline, regardless of the artifact's original size. This is the universal approach — it works identically in local and remote MCP deployments. For large artifacts like `manifest.json` on projects with thousands of models, this is the only practical option: the raw artifact is too large to process in context even when written to a temp file locally. Only valid for JSON artifacts. If the filter matches nothing, `[]` is returned.
- **output_path set**: Writes the full artifact to the specified local file path and returns a confirmation message. Use this when you want control over where the file lands — e.g. writing `manifest.json` directly to your project directory. **Important:** `output_path` is a write on the MCP server's filesystem, not the client's. When the server runs locally (stdio transport), this is your local machine and works as expected. When connected to a remote MCP server (SSE or streamable-HTTP transport), the path must be valid on the remote host — writing to a local client path like `/Users/you/...` will fail or produce a file you cannot access.
- **Both output_path and jq_filter set**: Returns an error — these parameters are mutually exclusive. Use `jq_filter` to filter inline, or `output_path` to write the full artifact to disk.

## jq Filter Reference

The three most-used artifacts have stable, well-known schemas. Common filters:

**Output shape:** jq results are always returned as a JSON array — the array contains each top-level value the filter emits. A filter that emits one object returns `[{...}]`; a filter that emits three strings returns `["a", "b", "c"]`; a filter that matches nothing returns `[]`. If you need just the inner value, read `result[0]` after parsing.

**Prefer aggregation over enumeration on large manifests.** Filters that enumerate all matching nodes (e.g. listing every table-materialized model by name) can return hundreds of kilobytes on large projects and exceed context limits. Aggregate or count when you don't need every name:
```
# Instead of listing all table-materialized models:
[.nodes | to_entries[] | select(.value.config.materialized == "table") | .value.name] | length
```

**run_results.json**
- All failed nodes: `.results[] | select(.status == "error") | {unique_id, message, execution_time}`
- All warned nodes: `.results[] | select(.status == "warn") | {unique_id, message}`
- Slowest 10 models: `[.results[] | {unique_id, execution_time}] | sort_by(-.execution_time) | .[0:10]`
- Summary counts: `.results | group_by(.status) | map({status: .[0].status, count: length})`

**manifest.json**
- All model names: `.nodes | keys[] | select(startswith("model."))`
- Models in a specific package: `.nodes | to_entries[] | select(.key | startswith("model.my_project.")) | .value.name`
- Models referencing a source: `.nodes | to_entries[] | select(.value.depends_on.nodes[] | contains("source.")) | .key`
- Tags on a model: `.nodes["model.my_project.my_model"].config.tags[]`

**catalog.json**
- Column names for a model: `.nodes["model.my_project.my_model"].columns | keys[]`
- All models with row counts: `.nodes | to_entries[] | {name: .key, rows: .value.stats.row_count.value}`

## Step Selection

By default, artifacts from the **last step** are returned — but this is often not what you want.

Two important caveats:

1. **Docs generation footgun**: Jobs that include a `dbt docs generate` step will have that step's compilation results as the default artifact, not the model execution results. `run_results.json` without a `step` will show all nodes as successful even when an earlier model run step had failures.

2. **Infrastructure steps produce no artifacts**: Steps like git clone, profile creation, and `dbt deps` do not write a `run_results.json`. Requesting an artifact from one of those steps raises a not-found error. The `step` parameter refers only to steps that run a dbt command (`dbt build`, `dbt run`, `dbt test`, `dbt docs generate`, etc.).

Always use `get_job_run_details` to inspect step names and statuses before specifying `step`, so you can identify which step index corresponds to the actual model execution.

- Step indexing starts at 1 for the first step

## Use Cases

- Inspect run_results.json for execution monitoring and test outcomes — use `step` to target the model run step specifically
- Retrieve manifest.json for lineage analysis or dbt Mesh audits — auto-written to a temp file on large projects; use `output_path` to control the destination
- Get catalog.json for documentation systems
- Access logs for debugging failed runs
- Inspect artifacts from non-production jobs not covered by the Discovery API (ad-hoc environments, test jobs, etc.)

## Example Usage

Fetch run results from a specific step (recommended when job includes docs generation):
```json
{
  "run_id": 789,
  "artifact_path": "run_results.json",
  "step": 4
}
```

Write a large manifest to disk instead of returning it as a string:
```json
{
  "run_id": 789,
  "artifact_path": "manifest.json",
  "output_path": "/tmp/manifest.json"
}
```

Fetch the default artifact (last step):
```json
{
  "run_id": 789,
  "artifact_path": "run_results.json"
}
```
