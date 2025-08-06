<instructions>
Gets compiled SQL for given metrics and dimensions/entities from the dbt Semantic Layer.

This tool generates the underlying SQL that would be executed for a given metric query by the `query_metrics` tool,
without actually running the query. This is useful for understanding what SQL is being
generated, debugging query issues, or getting SQL to run elsewhere.

To use this tool, you must first know about specific metrics, dimensions and
entities to provide. You can call the list_metrics, get_dimensions,
and get_entities tools to get information about which metrics, dimensions,
and entities to use.

When using the `group_by` parameter, ensure that the dimensions and entities
you specify are valid for the given metrics. Time dimensions can include
grain specifications (e.g., MONTH, DAY, YEAR).

The tool will return the compiled SQL that the dbt Semantic Layer would generate
to calculate the specified metrics with the given groupings.

Don't call this tool if the user's question cannot be answered with the provided
metrics, dimensions, and entities. Instead, clarify what metrics, dimensions,
and entities are available and suggest a new question that can be answered
and is approximately the same as the user's question.

This tool is particularly useful when:
- Users want to see the underlying SQL for a metric calculation
- Debugging complex metric definitions
- Understanding how grouping affects the generated SQL
- Getting SQL to run in other tools or systems
</instructions>

Returns the compiled SQL as a string, or an error message if the compilation fails.