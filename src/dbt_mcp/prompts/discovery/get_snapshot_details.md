<instructions>
Fetch detailed metadata for a dbt snapshot.

- Supply at least one of `unique_id` or `name`. Call will fail if both are missing.
- Always prefer providing `unique_id` for exact matches.
</instructions>

<examples>
1. Fetch a snapshot by unique_id:
   get_snapshot_details(unique_id="snapshot.analytics.customers")

2. Fetch a snapshot when only the node name is known:
   get_snapshot_details(name="customers")
</examples>
