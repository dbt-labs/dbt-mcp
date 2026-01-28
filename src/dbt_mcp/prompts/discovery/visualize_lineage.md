Renders an interactive visualization of the dbt lineage graph.

This tool displays a visual DAG (Directed Acyclic Graph) showing the relationships between dbt resources. The visualization is rendered inline in the chat interface.

**Parameters:**
- `unique_id`: **Required** - Full unique ID of the resource to center the visualization on (e.g., "model.my_project.customers")
- `types`: *Optional* - List of resource types to include in results. Defaults to all types EXCEPT Test.
  - Valid types: `Model`, `Source`, `Seed`, `Snapshot`, `Exposure`, `Metric`, `SemanticModel`, `SavedQuery`, `Test`
- `depth`: *Optional* - The depth of the lineage graph to return (default: 2). Controls how many levels upstream and downstream to traverse.
- `include_tests`: *Optional* - Whether to include Test nodes (default: false). Tests are excluded by default to keep the graph manageable.

**Features:**
- Color-coded nodes by resource type (sources in green, models in blue, etc.)
- Interactive zoom and pan controls
- Click on nodes to see details
- Minimap for navigation in large graphs
- Target node is highlighted

**Example Usage:**
```python
# Visualize lineage for a model (depth=2, no tests)
visualize_lineage(unique_id="model.analytics.customers")

# Visualize only models and sources
visualize_lineage(unique_id="model.analytics.customers", types=["Model", "Source"])

# Visualize with more depth
visualize_lineage(unique_id="model.analytics.customers", depth=3)

# Include tests in the visualization
visualize_lineage(unique_id="model.analytics.customers", include_tests=True)
```

**Note:** This tool requires a host that supports MCP Apps (UI rendering). If the host doesn't support apps, use `get_lineage` instead for text-based output.
