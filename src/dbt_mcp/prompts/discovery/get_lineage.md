<instructions>
Retrieves lineage for a dbt resource in a single API call. Supports name-based search for models, sources, seeds, and snapshots. Other resource types require unique_id.

**Parameters:**
- `direction`: "ancestors" (upstream), "descendants" (downstream), or "both" (default)
- `types`: Optional filter for specific resource types (Model, Source, Seed, Snapshot, Exposure, Metric, Test, etc.)

**Response:**
- `target`: The resource queried (matchesMethod=true)
- `ancestors/descendants`: Dependency nodes (based on direction)
- `pagination`: Metadata (max 50 nodes per direction, truncation info)

**Resource Types:**
Models, Sources, Seeds, Snapshots, Exposures, Metrics, SemanticModels, SavedQueries, Tests, Macros
</instructions>

<parameters>
name: The name of the dbt resource to retrieve lineage for. Searches models, sources, seeds, and snapshots by name. NOTE: Exposures, tests, and metrics cannot be searched by name - use unique_id instead.
unique_id: The unique identifier of the resource (e.g., "model.my_project.customer_orders"). If provided, this will be used instead of name for a precise lookup. Required for exposures, tests, and metrics.
direction: Which lineage to fetch - "ancestors", "descendants", or "both" (default: "both").
types: Optional list of resource types to include in results. Valid values: Model, Source, Seed, Snapshot, Exposure, Metric, SemanticModel, SavedQuery, Macro, Test.
</parameters>

<important_limitation>
**Name Search:** Only works for Models, Sources, Seeds, Snapshots.

**Require unique_id:** Exposures, Tests, Metrics must use format: `exposure.project.name`, `test.project.name`, `metric.project.name`
</important_limitation>

<examples>
# Search by name (models, sources, seeds, snapshots)
get_lineage(name="customer_orders")
get_lineage(name="raw_customers", direction="ancestors")
get_lineage(name="orders_snapshot", direction="descendants")

# Filter by type
get_lineage(name="fct_orders", types=["Model", "Exposure"])

# Use unique_id (exposures, tests, metrics)
get_lineage(unique_id="exposure.my_project.finance_dashboard")
get_lineage(unique_id="test.my_project.unique_customers_id")
get_lineage(unique_id="metric.my_project.total_revenue")
</examples>
