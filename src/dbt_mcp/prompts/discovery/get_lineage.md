Retrieves the lineage graph for a dbt resource.

Returns all nodes connected to the specified resource, including both upstream dependencies (ancestors) and downstream dependents (descendants).

**Parameters:**
- `unique_id`: **Required** - Full unique ID of the resource (e.g., "model.my_project.customers")
- `types`: *Optional* - List of resource types to include in results. If not provided, includes all types.
  - Valid types: `Model`, `Source`, `Seed`, `Snapshot`, `Exposure`, `Metric`, `SemanticModel`, `SavedQuery`, `Test`
- `depth`: *Optional* - The depth of the lineage graph to return (default: 5). Controls how many levels upstream and downstream to traverse from the target node.

**Returns:**
A list of all nodes in the connected subgraph, where each node contains:
- `uniqueId`: The resource's unique identifier
- `name`: The resource name
- `resourceType`: The type of resource (Model, Source, etc.)
- `parentIds`: List of unique IDs that this resource directly depends on

**Example Response:**
```json
[
  {
    "uniqueId": "source.raw.users",
    "name": "users",
    "resourceType": "Source",
    "parentIds": []
  },
  {
    "uniqueId": "model.stg_customers",
    "name": "stg_customers",
    "resourceType": "Model",
    "parentIds": ["source.raw.users"]
  },
  {
    "uniqueId": "model.customers",
    "name": "customers",
    "resourceType": "Model",
    "parentIds": ["model.stg_customers"]
  }
]
```

**Usage Examples:**
```python
# Get complete lineage (all connected nodes, all types, default depth of 5)
get_lineage(unique_id="model.analytics.customers")

# Get lineage filtered to only models and sources
get_lineage(unique_id="model.analytics.customers", types=["Model", "Source"])

# Get only immediate neighbors (depth=1)
get_lineage(unique_id="model.analytics.customers", depth=1)

# Get deeper lineage for comprehensive analysis
get_lineage(unique_id="model.analytics.customers", depth=10)
```

**Traversing the Graph:**

The graph is represented by parent-child relationships. To navigate:

**Finding Upstream Dependencies (Parents):**
```python
# Direct: What does this node depend on?
target_node = find_node_by_id(result, "model.customers")
direct_parents = target_node["parentIds"]
# Result: ["model.stg_customers"]
```

**Finding Downstream Dependents (Children):**
```python
# Search: What depends on this node?
target_id = "model.customers"
direct_children = [
    node for node in result
    if target_id in node.get("parentIds", [])
]
# Result: nodes that list "model.customers" in their parentIds
```

**Understanding the Results:**

- The target node is always included in the response
- All returned nodes are connected to the target (no disconnected nodes)
- To get full lineage, omit the `types` parameter
- To reduce payload size, specify relevant `types`

**Common Use Cases:**

1. **Impact Analysis**: "What will break if I change this model?"
   - Look at downstream dependents (nodes that have target in `parentIds`)

2. **Dependency Tracking**: "What does this model depend on?"
   - Look at upstream dependencies (`parentIds` of the target node)

3. **Data Lineage**: "Show the complete data flow for this entity"
   - Use all returned nodes to build a complete graph

4. **Finding Tests**: "What tests exist for this model and its dependencies?"
   - Filter results where `resourceType == "Test"`
