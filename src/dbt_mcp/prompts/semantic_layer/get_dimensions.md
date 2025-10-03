<instructions>
Get the dimensions for specified metrics

Dimensions are the attributes, features, or characteristics
that describe or categorize data.
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

<parameters>
metrics: List of metric names
search: Optional string used to filter dimensions by name using partial matches (only use when absolutely necessary as some dimensions might be missed due to specific naming styles)
</parameters>