<instructions>
Retrieves the parent models of a specific dbt model. These are the models that the specified model depends on.

You can provide either a name or a uniqueId, if known, to identify the model. Using uniqueId is more precise and guarantees a unique match, which is especially useful when models might have the same name in different projects.

Returned parents include `resourceType`, `name`, and `description`. For upstream sources, also provide `sourceName` and `uniqueId` so lineage can be linked back via `get_all_sources`.

This is specifically for retrieving model parents from the production manifest. If you want development lineage, use `get_model_lineage_dev` instead.
</instructions>

<parameters>
name: The name of the dbt model to retrieve parents for.
uniqueId: The unique identifier of the model. If provided, this will be used instead of name for a more precise lookup. You can get the uniqueId values for all models from the get_all_models() tool.
</parameters>

<examples>
1. Getting parents for a model by name:
   get_model_parents(name="customer_orders")

2. Getting parents for a model by uniqueId (more precise):
   get_model_parents(name="customer_orders", uniqueId="model.my_project.customer_orders")

3. Getting parents using only uniqueId:
   get_model_parents(uniqueId="model.my_project.customer_orders")
</examples>
