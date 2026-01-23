get_model_lineage_dev

<instructions>
Retrieves the lineage of a specific dbt resource from the local development manifest. Returns both upstream (parents) and downstream (children) dependencies.

This tool ONLY pulls from the local development manifest. For production lineage, use `get_lineage` instead.
</instructions>

<parameters>
unique_id: str => Fully-qualified unique ID of the resource (e.g. `model.analytics.stg_orders`).
types: list[LineageResourceType] | None = None => List of resource types to include. If None, includes all types. Valid types: Model, Source, Seed, Snapshot, Exposure, Metric, Test.
depth: int = 5 => The depth of the lineage graph to return. Controls how many levels to traverse (1 = immediate neighbors only).
</parameters>

<examples>
1. Getting full lineage for a model:
   get_model_lineage_dev(unique_id="model.my_project.customer_orders")

2. Getting lineage with depth control:
   get_model_lineage_dev(unique_id="model.my_project.customer_orders", depth=3)

3. Getting lineage filtered to only models and sources:
   get_model_lineage_dev(unique_id="model.my_project.customer_orders", types=["Model", "Source"])

4. Getting lineage excluding tests:
   get_model_lineage_dev(unique_id="model.my_project.customer_orders", types=["Model", "Source", "Seed", "Snapshot", "Exposure"])
</examples>
