<instructions>
Retrieves all upstream dependencies (ancestors) for a specific dbt model. This shows what the model depends on - sources, seeds, snapshots, and other models at any depth in the dependency tree.

**What's included:**
- **ancestors**: All upstream dependencies (sources, seeds, snapshots, models) at any depth
- **model info**: Basic information about the model itself (name, uniqueId, description, resourceType)

**Use this tool when:** You need to understand what data sources and models feed into a specific model, or trace back to the root data sources.

You can provide either a model_name or a uniqueId to identify the model. Using uniqueId is more precise and guarantees a unique match, which is especially useful when models might have the same name in different projects.

For upstream sources in the ancestors list, the response includes `sourceName` and `uniqueId` so lineage can be linked back via `get_all_sources`.
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

3. Interpreting the response structure:
   The response includes:
   - name, uniqueId, description, resourceType: Basic model information
   - ancestors: List of all upstream dependencies (all levels)

   Example response:
   {
     "name": "customer_orders",
     "uniqueId": "model.my_project.customer_orders",
     "description": "Customer order history",
     "resourceType": "model",
     "ancestors": [
       {"name": "customers", "resourceType": "model", ...},
       {"name": "orders", "resourceType": "source", "sourceName": "raw", ...}
     ]
   }
</examples>
