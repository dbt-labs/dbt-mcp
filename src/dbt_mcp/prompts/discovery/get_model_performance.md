<instructions>
Retrieve execution performance metrics for a dbt model from historical runs.

This tool queries the Discovery API to fetch model execution data including:
- Execution time (duration in seconds)
- Run status (success, error, skipped)
- Execution timestamps
- Associated job run IDs
- Optional: Test execution history (when include_tests=True)

Use this to:
- Analyze model performance trends over time
- Identify performance regressions
- Optimize slow-running models
- Debug execution issues
- Monitor test execution alongside model runs

The `unique_id` parameter is strongly preferred over `name` for deterministic lookups,
especially in projects with multiple packages.

If a name matches multiple models, the tool will raise an error and ask for a `unique_id`.

Results are always returned as an array (possibly empty), sorted newest-first.
- When `num_runs=1` (default), the array has at most one element (the latest run).
- When `num_runs > 1`, the array includes up to `num_runs` historical runs.

When `include_tests=True`, each run will include a `tests` array containing:
- Test name
- Test status (pass, fail, warn, error, skipped)
- Test execution time (duration in seconds)

Example unique_id format: model.analytics.stg_orders
</instructions>

<parameters>
unique_id: The unique identifier of the model (format: "model.project_name.model_name"). STRONGLY RECOMMENDED when available.
name: The name of the dbt model. Only use this when unique_id is unavailable.
num_runs: Number of historical runs to return (1-100). Default is 1 (latest run only). Use values > 1 to analyze performance trends over time.
include_tests: If True, include test execution history for each run. Default is False to reduce response size. Set to True when analyzing test performance or debugging test failures.
</parameters>

<examples>
1. PREFERRED METHOD - Get latest run performance using unique_id:
   get_model_performance(unique_id="model.analytics.stg_orders")

2. Analyze performance trend over last 10 runs:
   get_model_performance(unique_id="model.analytics.fct_orders", num_runs=10)

3. Get performance with test execution history:
   get_model_performance(unique_id="model.analytics.stg_orders", num_runs=5, include_tests=True)

4. FALLBACK METHOD - Using only name (only when unique_id is unknown):
   get_model_performance(name="stg_orders", num_runs=5)
</examples>
