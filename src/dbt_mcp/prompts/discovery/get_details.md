<instructions>
Retrieves detailed information for any dbt resource type: model, source, exposure, test, seed, snapshot, macro, or semantic_model.

IMPORTANT: Use unique_id when available.
- Using unique_id guarantees the correct resource is retrieved
- Using only name may return incorrect results if multiple resources share a name
- If you obtained resources via get_all_models(), get_all_sources(), etc., always use the unique_id from those results
- The resource_type is encoded in the unique_id prefix (e.g. `model.my_project.orders` → model, `source.my_project.raw.customers` → source)

Supply at least one of `unique_id` or `name`. The call will fail if both are missing.
</instructions>

<examples>
1. PREFERRED METHOD - Using unique_id (always use when available):
   get_details(resource_type="model", unique_id="model.my_project.customer_orders")
   get_details(resource_type="source", unique_id="source.my_project.raw_data.customers")
   get_details(resource_type="exposure", unique_id="exposure.my_project.customer_dashboard")
   get_details(resource_type="macro", unique_id="macro.dbt_utils.date_spine")
   get_details(resource_type="semantic_model", unique_id="semantic_model.my_project.orders")

2. FALLBACK METHOD - Using only name (only when unique_id is unknown):
   get_details(resource_type="model", name="customer_orders")
   get_details(resource_type="test", name="not_null_orders_order_id")
</examples>
