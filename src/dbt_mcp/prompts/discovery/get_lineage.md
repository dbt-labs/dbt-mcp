<instructions>
Retrieves lineage for a dbt resource using recursive traversal of parent/child relationships. Supports name-based search for models, sources, seeds, and snapshots. Other resource types require unique_id.

**Parameters:**
- `types`: **Required** list of resource types to include in results. Use empty list [] to include all types.
- `direction`: "ancestors" (upstream), "descendants" (downstream), or "both" (default)
- `depth`: **Required** Maximum traversal depth. 1 = direct parents/children only, 100 = full lineage.

**Response:**
- `target`: The queried resource (uniqueId, name, description, resourceType, database, schema, tags, fqn, parents, children).
- `ancestors/descendants`: Recursively discovered nodes (uniqueId, name, description, resourceType).
- `pagination`: Metadata (limit=50, total counts, truncation flags).

**Resource Types:**
Models, Sources, Seeds, Snapshots, Exposures, Metrics, SemanticModels, SavedQueries, Tests, Macros
</instructions>

<parameters>
name: The name of the dbt resource to retrieve lineage for. Searches models, sources, seeds, and snapshots by name. NOTE: Exposures, tests, and metrics cannot be searched by name - use unique_id instead.
unique_id: The unique identifier of the resource (e.g., "model.my_project.customer_orders"). If provided, this will be used instead of name for a precise lookup. Required for exposures, tests, and metrics.
types: **Required** list of resource types to include in results. Use empty list [] to see all types. Valid values: Model, Source, Seed, Snapshot, Exposure, Metric, SemanticModel, SavedQuery, Macro, Test.
direction: Which lineage to fetch - "ancestors", "descendants", or "both" (default: "both").
depth: **Required** Maximum traversal depth. 1 = direct parents/children only, 2 = two levels, 100 = full lineage.
</parameters>

<important_limitation>
**Name Search:** Only works for Models, Sources, Seeds, Snapshots.

**Require unique_id:** Exposures, Tests, Metrics must use format: `exposure.project.name`, `test.project.name`, `metric.project.name`
</important_limitation>

<examples>
# Full lineage (all types, all levels)
get_lineage(name="customer_orders", types=[], depth=100)

# Direct parents only (replaces get_model_parents)
get_lineage(name="orders", types=[], direction="ancestors", depth=1)

# Direct children only (replaces get_model_children)
get_lineage(name="orders", types=[], direction="descendants", depth=1)

# Two levels of ancestors
get_lineage(name="fct_orders", types=["Model"], direction="ancestors", depth=2)

# Filter by specific types
get_lineage(name="fct_orders", types=["Model", "Exposure"], depth=100)

# Use unique_id for exposures, tests, metrics
get_lineage(unique_id="exposure.my_project.finance_dashboard", types=["Model", "Source"], depth=100)
</examples>
