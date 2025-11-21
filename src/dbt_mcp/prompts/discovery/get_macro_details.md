<instructions>
Fetch detailed metadata for a dbt macro.

- Supply at least one of `unique_id` or `name`. Call will fail if both are missing.
- Always prefer providing `unique_id` for exact matches.
</instructions>

<examples>
1. Fetch a macro by unique_id:
   get_macro_details(unique_id="macro.dbt_utils.date_spine")

2. Fetch a macro when only the node name is known:
   get_macro_details(name="date_spine")
</examples>
