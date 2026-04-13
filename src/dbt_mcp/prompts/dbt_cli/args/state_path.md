Path to a directory containing dbt artifacts (for example, target/) from a previous run, which dbt uses as the source “state” for selection, deferral, and cloning behavior. 

IMPORTANT: dbt CLI (dbt Cloud CLI) does not support the `--state` flag. If `--state` is passed to the dbt CLI, it will be dropped from the final dbt command. State path is supported in dbt Core and dbt Fusion.