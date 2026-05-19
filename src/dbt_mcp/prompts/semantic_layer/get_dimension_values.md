<instructions>
Get the distinct values for a given dimension, optionally scoped to specific metrics.

Use this tool to discover what values exist for a dimension before building a `where`
filter in `query_metrics`. For example, to filter by country you first call this tool
to see which country values exist, then use those values in the `where` clause.

If the response includes `truncated: true`, there are more values than the current
`limit`. Increase `limit` to retrieve more, or add `metrics` to narrow the scope.
</instructions>

<examples>
<example>
Question: "I want to filter revenue by country — what countries are available?"
Thinking step-by-step:
   - The user wants to know valid values for a country dimension
   - I should scope to the "revenue" metric for accurate results
Parameters:
    dimension="customer__country"
    metrics=["revenue"]
    limit=100
</example>

<example>
Question: "What are the possible order statuses?"
Thinking step-by-step:
   - The user wants all distinct values for an order status dimension
   - No specific metric context needed
Parameters:
    dimension="order__status"
    metrics=null
    limit=100
</example>
</examples>
