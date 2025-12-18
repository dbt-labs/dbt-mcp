<instructions>
Retrieves lineage for a dbt resource using recursive traversal of parent/child relationships. Supports name-based search for models, sources, seeds, and snapshots. Other resource types require unique_id.

**Parameters:**
- `types`: **Required** list of resource types to include in results. Use empty list [] to include all types.
- `direction`: "ancestors" (upstream), "descendants" (downstream), or "both" (default)

**Response:**
- `target`: The resource queried with its direct parents and children.
- `ancestors/descendants`: Recursively discovered dependency nodes (based on direction and type filter).
- `pagination`: Metadata (max 50 nodes per direction, truncation info).

**Resource Types:**
Models, Sources, Seeds, Snapshots, Exposures, Metrics, SemanticModels, SavedQueries, Tests, Macros
</instructions>

<parameters>
name: The name of the dbt resource to retrieve lineage for. Searches models, sources, seeds, and snapshots by name. NOTE: Exposures, tests, and metrics cannot be searched by name - use unique_id instead.
unique_id: The unique identifier of the resource (e.g., "model.my_project.customer_orders"). If provided, this will be used instead of name for a precise lookup. Required for exposures, tests, and metrics.
types: **Required** list of resource types to include in results. Use empty list [] to see all types. Valid values: Model, Source, Seed, Snapshot, Exposure, Metric, SemanticModel, SavedQuery, Macro, Test.
direction: Which lineage to fetch - "ancestors", "descendants", or "both" (default: "both").
</parameters>

<important_limitation>
**Name Search:** Only works for Models, Sources, Seeds, Snapshots.

**Require unique_id:** Exposures, Tests, Metrics must use format: `exposure.project.name`, `test.project.name`, `metric.project.name`
</important_limitation>

<examples>
# See all lineage (all types)
get_lineage(name="customer_orders", types=[])

# Filter by specific types
get_lineage(name="fct_orders", types=["Model", "Exposure"])
get_lineage(name="raw_customers", types=["Model", "Source", "Seed"], direction="ancestors")

# Only models
get_lineage(name="orders_snapshot", types=["Model"], direction="descendants")

# Use unique_id for exposures, tests, metrics
get_lineage(unique_id="exposure.my_project.finance_dashboard", types=["Model", "Source"])
get_lineage(unique_id="test.my_project.unique_customers_id", types=[])
get_lineage(unique_id="metric.my_project.total_revenue", types=["Model", "Metric"])
</examples>
