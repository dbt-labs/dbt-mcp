<instructions>
Retrieves all downstream dependencies (descendants) for a specific dbt model. This shows what models, metrics, and exposures depend on this model at any depth in the dependency tree.

**What's included:**
- **descendants**: All downstream dependencies (models, metrics, exposures) at any depth
- **model info**: Basic information about the model itself (name, uniqueId, description, resourceType)

**Use this tool when:** You need to understand the impact of changes to a model - what other models and reports will be affected if this model changes.

You can provide either a model_name or a uniqueId to identify the model. Using uniqueId is more precise and guarantees a unique match, which is especially useful when models might have the same name in different projects.

**Performance note:** This tool uses breadth-first search (BFS) traversal to efficiently fetch all downstream dependencies. By default, it will traverse up to 50 levels deep and collect up to 1000 descendant nodes.
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

3. Interpreting the response structure:
   The response includes:
   - name, uniqueId, description, resourceType: Basic model information
   - descendants: List of all downstream dependencies (all levels)

   Example response:
   {
     "name": "customer_orders",
     "uniqueId": "model.my_project.customer_orders",
     "description": "Customer order history",
     "resourceType": "model",
     "descendants": [
       {"name": "customer_metrics", "resourceType": "model", ...},
       {"name": "revenue_report", "resourceType": "model", ...}
     ]
   }
</examples>
