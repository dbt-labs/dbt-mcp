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

- **Default**: Returns content inline for artifacts under 500 KB. For larger artifacts, writes to `~/.dbt/artifacts/run_{run_id}_{name}_{hash}_step{step}.{ext}` (where `{hash}` is an 8-character collision-avoidance suffix) and returns the path. Repeated calls with the same arguments overwrite the same file.
- **jq_filter set**: Applies a jq filter and returns results as a JSON array inline. Works in both local and remote deployments. Required for large artifacts like `manifest.json` that exceed context limits. Only valid for JSON artifacts; returns `[]` on no match. Filtered output is also capped at 500 KB — use aggregation filters if you hit the limit.

## jq Filter Reference

**Output shape:** Results are always a JSON array — one element per value the filter emits. A single-match filter returns `[{...}]`; no match returns `[]`. Read `result[0]` to unwrap a single value.

**Double-wrap:** Filters that construct a jq array (`[...]`, `group_by | map(...)`) produce `[[...]]`. Prefer iterator-style filters (`.results[] | select(...)`) for flat output.

**Prefer aggregation over enumeration on large manifests** — enumerating all matching nodes can produce hundreds of kilobytes and exceed the 500 KB output cap:
```
[.nodes | to_entries[] | select(.value.config.materialized == "table")] | length
```

**Top-level keys by artifact:**
- `run_results.json` — `.results[]` (array of node results with `.status`, `.unique_id`, `.execution_time`, `.message`)
- `manifest.json` — `.nodes` (object keyed by `"model.project.name"`), `.sources`, `.exposures`, `.metrics`
- `catalog.json` — `.nodes["model.project.name"].columns`, `.nodes["model.project.name"].stats`

## Step Selection

By default, artifacts from the **last step** are returned — but this is often not what you want.

Two important caveats:

1. **Docs generation footgun**: Jobs that include a `dbt docs generate` step will have that step's compilation results as the default artifact, not the model execution results. `run_results.json` without a `step` will show all nodes as successful even when an earlier model run step had failures.

2. **Infrastructure steps produce no artifacts**: Steps like git clone, profile creation, and `dbt deps` do not write a `run_results.json`. Requesting an artifact from one of those steps raises a not-found error. The `step` parameter refers only to steps that run a dbt command (`dbt build`, `dbt run`, `dbt test`, `dbt docs generate`, etc.).

Always use `get_job_run_details` to inspect step names and statuses before specifying `step`, so you can identify which step index corresponds to the actual model execution.

- Step indexing starts at 1 for the first step
