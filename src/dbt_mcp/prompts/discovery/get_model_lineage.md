<instructions>
Retrieves the complete lineage graph (all ancestors and descendants) for a specific dbt model in a single API call. This is more efficient than calling get_model_parents() and get_model_children() multiple times to build a full lineage tree.

**What's included:**
- **ancestors**: All upstream dependencies (sources, seeds, snapshots, models) at any depth
- **descendants**: All downstream dependencies (models, metrics, exposures) at any depth
- **model info**: Basic information about the model itself (name, uniqueId, description, resourceType)

**Performance benefit:** Instead of making N sequential API calls to traverse a lineage tree of depth N, this tool makes a single call and the server handles all traversal asynchronously.

You can provide either a model_name or a uniqueId to identify the model. Using uniqueId is more precise and guarantees a unique match, which is especially useful when models might have the same name in different projects.

For upstream sources in the ancestors list, the response includes `sourceName` and `uniqueId` so lineage can be linked back via `get_all_sources`.
</instructions>

<parameters>
model_name: The name of the dbt model to retrieve complete lineage for.
uniqueId: The unique identifier of the model. If provided, this will be used instead of model_name for a more precise lookup. You can get the uniqueId values for all models from the get_all_models() tool.
</parameters>

<examples>
1. Getting complete lineage for a model by name:
   get_model_lineage(model_name="customer_orders")

2. Getting complete lineage for a model by uniqueId (more precise):
   get_model_lineage(uniqueId="model.my_project.customer_orders")

3. Interpreting the response structure:
   The response includes:
   - name, uniqueId, description, resourceType: Basic model information
   - ancestors: List of all upstream dependencies (all levels)
   - descendants: List of all downstream dependencies (all levels)

   Example response:
   {
     "name": "customer_orders",
     "uniqueId": "model.my_project.customer_orders",
     "description": "Customer order history",
     "resourceType": "model",
     "ancestors": [
       {"name": "customers", "resourceType": "model", ...},
       {"name": "orders", "resourceType": "source", "sourceName": "raw", ...}
     ],
     "descendants": [
       {"name": "customer_metrics", "resourceType": "model", ...},
       {"name": "revenue_report", "resourceType": "model", ...}
     ]
   }
</examples>
