<instructions>
Retrieves all upstream dependencies (ancestors) for a specific dbt model at any depth in the dependency tree. This shows all sources, seeds, snapshots, and models that this model depends on, directly or indirectly.

You can provide either a model_name or a uniqueId, if known, to identify the model. Using uniqueId is more precise and guarantees a unique match, which is especially useful when models might have the same name in different projects.

Returns the model info plus an `ancestors` list containing all upstream dependencies. For upstream sources, the response includes `sourceName` and `uniqueId` so lineage can be linked back via `get_all_sources`.
</instructions>

<parameters>
model_name: The name of the dbt model to retrieve ancestors for.
uniqueId: The unique identifier of the model. If provided, this will be used instead of model_name for a more precise lookup. You can get the uniqueId values for all models from the get_all_models() tool.
</parameters>

<examples>
1. Getting ancestors for a model by name:
   get_model_ancestors(model_name="customer_orders")

2. Getting ancestors for a model by uniqueId (more precise):
   get_model_ancestors(uniqueId="model.my_project.customer_orders")

3. Getting ancestors using only uniqueId:
   get_model_ancestors(uniqueId="model.my_project.customer_orders")
</examples>
