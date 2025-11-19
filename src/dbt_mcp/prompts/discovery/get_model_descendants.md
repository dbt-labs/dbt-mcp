<instructions>
Retrieves all downstream dependencies (descendants) for a specific dbt model at any depth in the dependency tree. This shows all models, metrics, and exposures that depend on this model, directly or indirectly.

You can provide either a model_name or a uniqueId, if known, to identify the model. Using uniqueId is more precise and guarantees a unique match, which is especially useful when models might have the same name in different projects.

Returns the model info plus a `descendants` list containing all downstream dependencies. By default, traverses up to 50 levels deep and collects up to 1000 nodes. If these limits are reached, a `warnings` field will explain what was truncated.
</instructions>

<parameters>
model_name: The name of the dbt model to retrieve descendants for.
uniqueId: The unique identifier of the model. If provided, this will be used instead of model_name for a more precise lookup. You can get the uniqueId values for all models from the get_all_models() tool.
</parameters>

<examples>
1. Getting descendants for a model by name:
   get_model_descendants(model_name="customer_orders")

2. Getting descendants for a model by uniqueId (more precise):
   get_model_descendants(uniqueId="model.my_project.customer_orders")

3. Getting descendants using only uniqueId:
   get_model_descendants(uniqueId="model.my_project.customer_orders")
</examples>
