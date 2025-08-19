<instructions>
Retrieves information about the health of a dbt model, including the last time it ran, the last test execution status, and whether the upstream data for the model is fresh.

IMPORTANT: Use uniqueId when available.
- Using uniqueId guarantees the correct model is retrieved
- Using only model_name may return incorrect results or fail entirely
- If you obtained models via get_all_models(), you should always use the uniqueId from those results

ASSESSING MODEL HEALTH: 
For all of the below, summarize whether the model is healthy, questionable, or unhealthy. Only provide more details when asked.

- for the model executionInfo, if the lastRunStatus is "success" consider the model healthy
- for the test executionInfo, if the lastRunStatus is "success" consider the model healthy

- for the models parents:
-- check the modelexecutionInfo, snapshotExecutionInfo, and seedExecutionInfo. If the lastRunStatus is "success" consider the model healthy. If the lastRunStatus is "error" consider the model unhealthy.

-- if the parent node is a SourceAppliedStateNestedNode:
--- If the freshnessStatus is "pass", consider the model healthy
--- If the freshnessStatus is "fail", consider the model unhealthy
--- If the freshnessStatus is null, consider the model health questionable
--- If the freshnessStatus is "warn", consider the model health questionable
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