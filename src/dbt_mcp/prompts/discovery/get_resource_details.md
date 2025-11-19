<instructions>
Fetches applied resource details for a specific dbt object type (models, sources, exposures, tests, seeds, snapshots, macros, or semantic models). Results include the fields exposed by the Metadata API for that resource, such as compiled SQL for models, freshness for sources, execution info for snapshots, and measure/dimension definitions for semantic models.

- Always provide `resource_type` to select the desired resource family.
- Prefer unique IDs when available; they guarantee the returned node matches what downstream tools expect.
- Use `unique_id` for a single resource or `unique_ids` for multiple resources of the same type.
- If a name is all you have, resolve it to a unique ID with the other discovery tools first.
</instructions>

<parameters>
resource_type: One of `model`, `source`, `exposure`, `test`, `seed`, `snapshot`, `macro`, or `semantic_model`. Required.
unique_id: Single unique identifier for the resource. Optional when `unique_ids` is supplied.
unique_ids: List of unique identifiers for bulk lookups of the same type. Optional.
</parameters>

<examples>
1. Fetch a single model by unique ID:
   get_resource_details(resource_type="model", unique_id="model.analytics.stg_orders")

2. Retrieve several snapshots at once:
   get_resource_details(
       resource_type="snapshot",
       unique_ids=["snapshot.analytics.daily_orders", "snapshot.analytics.weekly_users"],
   )

3. Inspect a macro definition:
   get_resource_details(resource_type="macro", unique_id="macro.analytics.generate_calendar")
</examples>
