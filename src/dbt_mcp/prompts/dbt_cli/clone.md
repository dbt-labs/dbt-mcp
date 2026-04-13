Use this command to clone selected dbt nodes from a prior state (artifacts from an earlier run, typically production) into the current target schema(s).

Behavior:

- Source state
  - The `--state` parameter must point to a directory containing dbt artifacts (especially `manifest.json`) from a previous run, usually a successful production job.
  - CLIs that DO NOT connect to dbt Platform do not need to pass this parameter (ie self-ran dbt-core)

- Idempotency / overwrites
  - By default, pre-existing relations in the target are not recreated
  - Use the `--full-refresh` parameter so dbt recreates relations even if they already exist.

- Typical use cases
  - Blue/green or other continuous deployment patterns on warehouses that support zero-copy cloning.  
  - Cloning current production state into development schemas for realistic dev/testing.  
  - Handling incremental models in CI** (clone instead of full-refreshing heavy incrementals on supported warehouses).  
  - Testing code changes in downstream tools (BI, notebooks, etc.) against real, cloned tables/views.