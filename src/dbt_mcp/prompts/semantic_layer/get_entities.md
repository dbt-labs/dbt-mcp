<instructions>
Get the entities for specified metrics

Entities are real-world concepts in a business such as customers,
transactions, and ad campaigns. Analysis is often focused around
specific entities, such as customer churn or
annual recurring revenue modeling.
</instructions>

<examples>
<example>
Question: "I want to analyze revenue - what entities are available?"
Parameters:
    metrics=["revenue"]
    search=null
</example>

<example>
Question: "Are there any customer-related entities for my sales metrics?"
Thinking step-by-step:
   - The user is interested in customer entities specifically
   - I should use the search parameter to filter for entities with "customer" in the name
   - This will narrow down the results to just customer-related entities
Parameters:
    metrics=["total_sales", "average_order_value"]
    search="customer"
</example>
</examples>

<parameters>
metrics: List of metric names
search: Optional string used to filter entities by name using partial matches (only use when absolutely necessary as some entities might be missed due to specific naming styles)
</parameters>