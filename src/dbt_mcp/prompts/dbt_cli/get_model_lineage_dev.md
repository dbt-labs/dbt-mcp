get_model_lineage_dev

<instructions>
Retrieves the model lineage of a specific dbt model, it allows for upstream, downstream, or both. These are the models that depend on the specified model.

You can provide either a model_name or a uniqueId, if known, to identify the model. Using uniqueId is more precise and guarantees a unique match, which is especially useful when models might have the same name in different projects.
This specifically ONLY pulls from the local development manifest.  If you want production lineage, use `get_model_children` or `get_model_parents` instead.
</instructions>

<parameters>
model_id: str => Either the uniqueId or the `identifier` of the dbt model to retrieve lineage for.
direction: Literal["parents", "children", "both"] = "both"  => The direction of lineage to retrieve. "parents" for upstream models, "children" for downstream models, and "both" for both directions.
exclude_prefixes: tuple[str, ...] = ("test.", "unit_test."), => A tuple of prefixes to exclude from the lineage results. Assets with identifiers starting with any of these prefixes will be ignored.
recursive: bool = False => Whether to retrieve lineage recursively. If set to True, it will fetch all levels of lineage in the specified direction(s).
</parameters>

<examples>
1. Getting children for a model by name:
   get_model_lineage_dev(model_id="customer_orders", direction="children")

2. Getting parents for a model by uniqueId (more precise):
   get_model_lineage_dev(model_id="model.my_project.customer_orders", direction="parents")

3. Getting both upstream and downstream lineage recursively and including tests:
   get_model_lineage_dev(model_id="model.my_project.customer_orders", direction="both", exclude_prefixes=(), recursive=True)
</examples>
