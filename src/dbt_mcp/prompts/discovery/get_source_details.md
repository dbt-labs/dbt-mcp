<instructions>
Retrieves detailed information about a specific dbt source, including database, schema, identifier, freshness status, and column-level details (name, type, description).

IMPORTANT: Use uniqueId when available.
- Using uniqueId guarantees the correct source table is retrieved
- Using only name may return incorrect results if multiple sources have tables with the same name
- If you obtained sources via get_all_sources(), you should always use the uniqueId from those results
</instructions>

<parameters>
uniqueId: The unique identifier of the source (format: "source.project_name.source_name.table_name"). STRONGLY RECOMMENDED when available.
name: The table name within the source. Only use this when uniqueId is unavailable.
</parameters>

<examples>
1. PREFERRED METHOD - Using uniqueId (always use this when available):
   get_source_details(uniqueId="source.my_project.raw_data.customers")

2. FALLBACK METHOD - Using only name (only when uniqueId is unknown):
   get_source_details(name="customers")
</examples>
