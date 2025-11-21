<instructions>
Fetch detailed metadata for a dbt MetricFlow semantic model.

- Supply at least one of `unique_id` or `name`. Call will fail if both are missing.
- Always prefer providing `unique_id` for exact matches.
</instructions>

<examples>
1. Fetch a semantic model by unique_id:
   get_semantic_model_details(unique_id="semantic_model.analytics.customers")

2. Fetch a semantic model when only the node name is known:
   get_semantic_model_details(name="customers")
</examples>
