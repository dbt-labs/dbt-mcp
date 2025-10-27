Get the name, description, and metadata of all dbt sources in the environment. Sources represent external data tables that your dbt models build upon.

Parameters (all optional):
- source_names: List of specific source names to filter by (e.g., ['raw_data', 'external_api'])
- unique_ids: List of specific source table IDs to filter by

Note:
  - source_names correspond to the top-level source grouping in the source YML config
  - unique_ids have the form `source.{YOUR-DBT-PROJECT}.{SOURCE-NAME}.{SOURCE-TABLE}`

Returns information including:
- name: The table name within the source
- uniqueId: The unique identifier for this source table
- identifier: The underlying table identifier in the warehouse
- description: Description of the source table
- sourceName: The source name (e.g., 'raw_data', 'external_api')
- database: Database containing the source table
- schema: Schema containing the source table
- resourceType: Will be 'source'
- freshness: Real-time freshness status from production including:
  - maxLoadedAt: When the source was last loaded
  - maxLoadedAtTimeAgoInS: How long ago the source was loaded (in seconds)
  - freshnessStatus: Current freshness status (e.g., 'pass', 'warn', 'error')

