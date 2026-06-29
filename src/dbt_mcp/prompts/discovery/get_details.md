Retrieves full details for a dbt resource by type and name or unique ID.

Use this tool when you need the full schema, definition, or metadata for a specific dbt resource. The `resource_type` determines what fields are returned.

**Parameters:**
- `resource_type`: The type of dbt resource. One of: `model`, `source`, `exposure`, `test`, `seed`, `snapshot`, `macro`, `semantic_model`. The resource type is also encoded in the `unique_id` prefix (e.g. `model.my_project.orders` → `model`).
- `name`: The resource's short name (e.g. `orders`). Provide `name` or `unique_id`, not both.
- `unique_id`: The resource's fully qualified unique ID (e.g. `model.my_project.orders`). Preferred when available — avoids ambiguity when multiple resources share a name.

**Usage examples:**
```python
# Get details for a model by unique_id
get_details(resource_type="model", unique_id="model.analytics.orders")

# Get details for a source by name
get_details(resource_type="source", name="raw_users")

# Get details for a semantic model
get_details(resource_type="semantic_model", unique_id="semantic_model.analytics.orders")
```
