<instructions>
Retrieves the child models (downstream dependencies) of a specific dbt model. These are the models that depend on the specified model.

You can provide either a model_name or a uniqueId, if known, to identify the model. Using uniqueId is more precise and guarantees a unique match, which is especially useful when models might have the same name in different projects.
</instructions>

<parameters>
model_name: The name of the dbt model to retrieve children for.
uniqueId: The unique identifier of the model. If provided, this will be used instead of model_name for a more precise lookup. You can get the uniqueId values for all models from the get_all_models() tool.
</parameters>

<examples>
1. Getting children for a model by name:
   get_model_children(model_name="customer_orders")

2. Getting children for a model by uniqueId (more precise):
   get_model_children(model_name="customer_orders", uniqueId="model.my_project.customer_orders")

3. Getting children using only uniqueId:
   get_model_children(uniqueId="model.my_project.customer_orders")
</examples>
