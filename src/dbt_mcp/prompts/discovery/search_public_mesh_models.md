Searches for public dbt Mesh models across producer projects, confirming each producer's identity so the returned `ref` is safe to paste.

**Scope:** only models with Mesh `access: public`, and only the projects named in `project_ids`. For a model's governance config, or for any model regardless of access level, use `get_node_details`. To audit your *own* project's public interface, use `get_all_models`.

**Two ways to search,** combinable: `query` (text over names/descriptions) and `relation` (exact warehouse relation lookup, given as `identifier`/`schema`/`database` fields rather than one dotted string). See each parameter's description for matching rules. `relation` narrows server-side before `query` applies, so a `query` alongside it can only exclude that one model from `models` — it never affects `lookup`. Omitting both returns every public model in scope.

**Returns** a `MeshModelSearchResult`:
- `models` — each with `ref` (the ready-to-use `{{ ref(...) }}` string), `dbt_project_name`, `relation_name` (the server's own verbatim relation string for that model — distinct from the input `relation` you searched for), `version`/`latest_version`, `last_run_status`/`last_run_generated_at` (raw passthrough, no staleness logic), `project_id`, and `unique_id`. Models the platform confirms belong to a *different* project (an installed package, say) are excluded.
- `dependencies` — one producer project name per entry, deduped: the `dependencies.yml` list.
- `coverage` — `searched`, `failed` (project → reason), `incomplete` (scan stopped with pages remaining), `models_scanned` (`null` on a `relation` lookup).
- `lookup` — only with `relation`: the echoed relation, `found`, `conclusive`.
- `omitted_model_count` — how many matches `limit` cut.

**Reading `lookup` before declaring a new source.** `found: false` is authoritative only when `conclusive: true`, which requires `coverage.failed` and `coverage.incomplete` to both be empty. Even then it means "not claimed by a project in `project_ids`", never "not claimed by any dbt model" — a private or protected model at that relation also yields `found: false`. So: put every candidate producer in `project_ids`, require `conclusive: true`, and use `get_node_details` to rule out a non-public owner.

**Sizing.** `models` is capped at `limit` across all projects, with no cursor for more; `lookup`, `coverage`, and `dependencies` are computed over the full scan before the cap. A nonzero `omitted_model_count` means narrow with `query` or `relation` rather than raise `limit`.
