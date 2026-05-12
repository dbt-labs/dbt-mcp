<instructions>
Retrieves the child models (downstream dependencies) of a specific dbt model. These are the models that depend on the specified model.

You can provide either a name or a uniqueId, if known, to identify the model. Using uniqueId is more precise and guarantees a unique match, which is especially useful when models might have the same name in different projects.

This is specifically for retrieving model children from the production manifest. If you want development lineage, use `get_lineage_dev` instead.
</instructions>

<examples>
1. Getting children for a model by name:
   get_model_children(name="customer_orders")

2. Getting children for a model by uniqueId (more precise):
   get_model_children(name="customer_orders", uniqueId="model.my_project.customer_orders")

3. Getting children using only uniqueId:
   get_model_children(uniqueId="model.my_project.customer_orders")
   </examples>
