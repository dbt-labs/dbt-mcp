<instructions>
Retrieves information about a specific dbt model, including compiled SQL, description, and column details.

IMPORTANT: Use uniqueId when available.
- Using uniqueId guarantees the correct model is retrieved
- Using only model_name may return incorrect results or fail entirely
- If you obtained models via get_all_models(), you should always use the uniqueId from those results

ASSESSING MODEL HEALTH: 
- for the model level executionInfo, if the lastRunStatus is "success" consider the model healthy
- for the test level executionInfo, if the lastRunStatus is "success" consider the model healthy

Also consider the source freshness of the model's parents:
- If the freshnessStatus is null, consider the model questionably health
- If the freshnessRunGeneratedAt is null or more than 24 hours old, consider the model questionably healthy
- If the freshnessStatus is "pass", consider the model healthy
- If the freshnessStatus is "fail", consider the model unhealthy
- If the freshnessStatus is "warn", consider the model questionably healthy
</instructions>

<parameters>
uniqueId: The unique identifier of the model (format: "model.project_name.model_name"). STRONGLY RECOMMENDED when available.
model_name: The name of the dbt model. Only use this when uniqueId is unavailable.
</parameters>

<examples>
1. PREFERRED METHOD - Using uniqueId (always use this when available):
   get_model_details(uniqueId="model.my_project.customer_orders")
   
2. FALLBACK METHOD - Using only model_name (only when uniqueId is unknown):
   get_model_details(model_name="customer_orders")
</examples>