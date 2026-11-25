Retrieves detailed information about a specific dbt node from the local development manifest using `dbt ls --output json`. Works for models, seeds, snapshots, sources, exposures, metrics, semantic models, saved queries, tests, and other dbt resource types.

Returns all available node metadata including alias, config (materialization, tags, freshness, etc.), depends_on (refs, macros), name, original_file_path, package_name, resource_type, tags, unique_id and more.

Some important information:
- patch_path: the path to the YAML file that configures the node.
- columns: list of columns configured in the node config. Not necessarily the entire list of columns in the node.
- tests: for models, seeds, sources, and snapshots, includes a list of associated test unique_ids.

This specifically ONLY pulls from the local development manifest. If you want production details, use the Discovery API tools (`get_model_details`, `get_seed_details`, `get_snapshot_details`, etc.) instead.

`node_id` accepts a node name, a method selector, or a unique_id. Unique IDs returned by `get_lineage_dev` (for example `model.my_project.stg_zip__invoices` or `source.my_project.raw.payments`) are resolved through the local manifest before `dbt list --select` runs. Names (`stg_zip__invoices`) and method selectors (`source:raw.payments`, `exposure:revenue_dashboard`) are passed through unchanged.

Examples:
- get_node_details_dev(node_id="stg_zip__invoices")
- get_node_details_dev(node_id="model.my_project.stg_zip__invoices")
- get_node_details_dev(node_id="seed.my_project.country_codes")
- get_node_details_dev(node_id="source:raw.payments")
- get_node_details_dev(node_id="source.my_project.raw.payments")
- get_node_details_dev(node_id="exposure:revenue_dashboard")
