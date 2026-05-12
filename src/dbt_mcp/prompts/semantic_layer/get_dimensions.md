<instructions>
Get the dimensions for specified metrics

Note: `metric_time` is a standard time dimension available on most metrics. You do not need to call this tool just to confirm time dimensions exist — call it only when you need categorical dimensions or specific granularity details. If this tool returns no results, proceed to query directly using `metric_time`.

Dimensions are the attributes, features, or characteristics
that describe or categorize data.

Each dimension includes a metadata field containing config.meta from semantic model YAML files.
The metadata contains fields relevant to the semantic models.

</instructions>

<examples>
<example>
Question: "I want to analyze revenue trends - what dimensions are available?"
Thinking step-by-step:
   - Using list_metrics(), I find "revenue" is available
   - Now I can get the dimensions for this metric
   - The search parameter is not needed here since the user is interested in all available dimensions.
Parameters:
    metrics=["revenue"]
    search=null
</example>

<example>
Question: "Are there any time-related dimensions for my sales metrics?"
Thinking step-by-step:
   - Using list_metrics(), I find "total_sales" and "average_order_value" are available
   - The user is interested in time dimensions specifically
   - I should use the search parameter to filter for dimensions with "time" in the name
   - This will narrow down the results to just time-related dimensions
Parameters:
    metrics=["total_sales", "average_order_value"]
    search="time"
</example>
</examples>
