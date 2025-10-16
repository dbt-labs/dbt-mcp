Get the name, description, and metadata of all dbt sources in the environment. Sources represent external data tables that your dbt models build upon.

Supports filtering by:
- sourceNames: Filter by specific source names (e.g., ['raw_data', 'external_api'])
- uniqueIds: Filter by specific source table IDs
- database: Filter by database name
- schema: Filter by schema name
- freshnessStatus: Filter by freshness status ('pass', 'warn', 'error')
- tags: Filter by source tags

Returns information including:
- name: The table name within the source
- uniqueId: The unique identifier for this source table
- description: Description of the source table
- sourceName: The source name (e.g., 'raw_data', 'external_api')
- resourceType: Will be 'source'
- freshness: Real-time freshness status from production including:
  - maxLoadedAt: When the source was last loaded
  - maxLoadedAtTimeAgoInS: How long ago the source was loaded (in seconds)
  - freshnessStatus: Current freshness status (e.g., 'pass', 'warn', 'error')

This tool is useful for:
- Data discovery and understanding available source tables
- Lineage analysis to see what external data feeds into your dbt project
- Source validation and freshness monitoring
- Filtering sources by environment, status, or organizational tags
- Getting a complete picture of your data graph including upstream dependencies