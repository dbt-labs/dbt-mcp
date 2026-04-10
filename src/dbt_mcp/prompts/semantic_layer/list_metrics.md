List metrics from the dbt Semantic Layer.

When the number of metrics is below the configured threshold (default: 10), each metric includes the names of its available dimensions and entities. Use get_dimensions or get_entities for full details (types, granularities, descriptions) on specific metrics.

When above the threshold, only metrics are returned. Use get_dimensions and get_entities with the specific metrics you need.

If the user is asking a data-related or business-related question, use this tool as a first step.

<parameters>
search: Optional string used to filter metrics by name using partial matches
</parameters>
