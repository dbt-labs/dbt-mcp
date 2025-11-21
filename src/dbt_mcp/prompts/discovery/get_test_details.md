<instructions>
Fetch detailed metadata for a dbt test.

- Supply at least one of `unique_id` or `name`. Call will fail if both are missing.
- Always prefer providing `unique_id` for exact matches.
</instructions>

<examples>
1. Fetch a test by unique_id:
   get_test_details(unique_id="test.analytics.is_empty_string")

2. Fetch a test when only the node name is known:
   get_test_details(name="is_empty_string")
</examples>
