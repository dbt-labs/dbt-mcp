Retrieves the lineage graph for a dbt resource.

Returns the subgraph of nodes connected to the specified resource, including both upstream dependencies (ancestors) and downstream dependents (descendants).

**Returns:**
A structured `LineageGraph` object:
- `root_id`: the `unique_id` the lineage was computed relative to (the target node).
- `nodes`: the connected nodes, each with:
  - `unique_id`: the resource's unique identifier
  - `name`: the resource name
  - `resource_type`: the type of resource (Model, Source, etc.)
- `edges`: the dependency edges, each with `source` and `target` `unique_id`s, where `source` is an upstream parent of `target` (data flows `source` → `target`).

The target node (`root_id`) is present in `nodes` for `direction="both"` (the default). For `direction="upstream"`/`"downstream"` the target is excluded, so `root_id` may not appear in `nodes`.

Call `get_lineage(unique_id=...)` to retrieve the target node plus only its immediate parents and children (the default `depth=1`).

Use `direction` to narrow the response to one side of the graph and reduce payload size:
- `direction="upstream"`: ancestors only — excludes the target node and descendants
- `direction="downstream"`: descendants only — excludes the target node and ancestors
- `direction="both"` (default): the target node plus both ancestors and descendants

`direction="upstream"`/`"downstream"` are drop-in replacements for `get_model_parents`/`get_model_children`: same node set, target excluded either way.

**Example Response:**
```json
{
  "type": "lineage_graph",
  "root_id": "model.customers",
  "nodes": [
    {"unique_id": "source.raw.users", "name": "users", "resource_type": "Source"},
    {"unique_id": "model.stg_customers", "name": "stg_customers", "resource_type": "Model"},
    {"unique_id": "model.customers", "name": "customers", "resource_type": "Model"}
  ],
  "edges": [
    {"source": "source.raw.users", "target": "model.stg_customers"},
    {"source": "model.stg_customers", "target": "model.customers"}
  ]
}
```

**Usage Examples:**
```python
# Get lineage (immediate parents and children — default depth of 1)
get_lineage(unique_id="model.analytics.customers")

# Get lineage filtered to only models and sources
get_lineage(unique_id="model.analytics.customers", types=["Model", "Source"])

# Get deeper lineage for comprehensive analysis
get_lineage(unique_id="model.analytics.customers", depth=10)

# Get the full connected graph (depth=0 traverses without a limit)
get_lineage(unique_id="model.analytics.customers", depth=0)

# Get only upstream dependencies (ancestors), e.g. for dependency tracking
get_lineage(unique_id="model.analytics.customers", direction="upstream")

# Get only downstream dependents (descendants), e.g. for impact analysis
get_lineage(unique_id="model.analytics.customers", direction="downstream")
```

**Traversing the Graph:**

Relationships are explicit in `edges` (`source` → `target`, parent → child).

**Finding Upstream Dependencies (Parents):** the parents of a node are the `edges` whose `target` is that node — read their `source`.

**Finding Downstream Dependents (Children):** the children of a node are the `edges` whose `source` is that node — read their `target`.

**Understanding the Results:**

- The target node is included only when `direction="both"` (the default); it is excluded for `direction="upstream"`/`"downstream"`
- All returned nodes are connected to the target (no disconnected nodes)
- To get full lineage, omit the `types` parameter
- To reduce payload size, specify relevant `types`

**Common Use Cases:**

1. **Impact Analysis**: "What will break if I change this model?"
   - Follow `edges` downstream from the target (edges where `source` is the target)

2. **Dependency Tracking**: "What does this model depend on?"
   - Follow `edges` upstream to the target (edges where `target` is the target node)

3. **Data Lineage**: "Show the complete data flow for this entity"
   - Use `nodes` and `edges` to build the complete graph

4. **Finding Tests**: "What tests exist for this model and its dependencies?"
   - Filter `nodes` where `resource_type == "Test"`
