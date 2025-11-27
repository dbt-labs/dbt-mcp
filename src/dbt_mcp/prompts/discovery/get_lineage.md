<instructions>
Retrieves lineage (ancestors and/or descendants) for a dbt resource using a single efficient API call with dbt selector syntax.

You can provide either a resource name or a unique_id to identify the resource. Using unique_id is more precise and guarantees a unique match, which is especially useful when resources might have the same name across different types (e.g., a model and source both named "customers").

The `direction` parameter controls what lineage is returned:
- "ancestors": upstream dependencies only (what this resource depends on)
- "descendants": downstream dependencies only (what depends on this resource)
- "both": all lineage in both directions (default)

The `types` parameter optionally filters which resource types to include in results.

The lineage can include these resource types:
- Model: dbt models with database, schema, materialization type
- Source: external data sources with source name, database, schema
- Seed: CSV files loaded as tables with database, schema
- Snapshot: point-in-time captures with database, schema
- Exposure: downstream consumers like dashboards and reports
- Metric: semantic layer metrics
- SemanticModel: semantic layer models with dimensions and measures
- SavedQuery: pre-defined semantic layer queries
- Test: data tests attached to models
- Macro: Jinja macros used in transformations

The response includes:
- target: the resource you queried for (identified by matchesMethod=true)
- ancestors: upstream dependencies (if direction is "ancestors" or "both")
- descendants: downstream dependencies (if direction is "descendants" or "both")
- pagination: metadata about result limits and truncation

**Limits:**
- Maximum 50 nodes returned per direction (ancestors/descendants)
- If results are truncated, `pagination` metadata shows total count and `truncated: true`
- Use `types` parameter to filter results if you need specific resource types
</instructions>

<parameters>
name: The name of the dbt resource to retrieve lineage for. Searches models first, then sources.
unique_id: The unique identifier of the resource (e.g., "model.my_project.customer_orders"). If provided, this will be used instead of name for a precise lookup.
direction: Which lineage to fetch - "ancestors", "descendants", or "both" (default: "both").
types: Optional list of resource types to include in results. Valid values: Model, Source, Seed, Snapshot, Exposure, Metric, SemanticModel, SavedQuery, Macro, Test.
</parameters>

<examples>
1. Getting all lineage (both directions) for a model:
   get_lineage(name="customer_orders")

2. Getting only ancestors (upstream dependencies):
   get_lineage(name="customer_orders", direction="ancestors")

3. Getting only descendants (downstream dependencies):
   get_lineage(unique_id="model.my_project.customer_orders", direction="descendants")

4. Getting lineage filtered to specific types:
   get_lineage(name="fct_orders", direction="descendants", types=["Model", "Exposure"])

5. Getting source lineage:
   get_lineage(unique_id="source.my_project.stripe.payments", direction="descendants")
</examples>
