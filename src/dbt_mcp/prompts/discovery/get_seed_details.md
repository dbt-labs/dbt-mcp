<instructions>
Fetch detailed metadata for a dbt seed.

- Supply at least one of `unique_id` or `name`. Call will fail if both are missing.
- Always prefer providing `unique_id` for exact matches.
</instructions>

<examples>
1. Fetch a seed by unique_id:
   get_seed_details(unique_id="seed.analytics.customers")

2. Fetch a seed when only the node name is known:
   get_seed_details(name="customers")
</examples>
