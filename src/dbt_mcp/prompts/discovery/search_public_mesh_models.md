Searches for public dbt Mesh models across producer projects and confirms each producer's identity, so you can build the exact cross-project `ref()` and `dependencies.yml` entry.

**Scope:** only models with Mesh `access: public`, and only the projects named in `project_ids` (which must be a subset of the configured project IDs). Private and protected models are never returned — use `get_node_details` to inspect a specific model regardless of access level. To audit your *own* project's public interface, use `get_all_models(access="public")` instead; it lists the bound environment's public models with no cross-project search. Follow up with `get_node_details` on any of them for `group`, `contractEnforced`, and the rest of a model's governance config.

**Identity verification is automatic.** For each project, the tool asks the platform for that environment's own `dbt_project.yml` name rather than guessing from a model's `unique_id`. When a model's namespace matches, `dbt_project_name` is populated. When the lookup fails — or when a model's `unique_id` carries no package to compare the answer against — `dbt_project_name` stays null, the affected model carries a warning naming which of the two happened, and, on a `relation_name` lookup, the project is listed in `coverage.unresolved_identity`.

**Two ways to search:**
- `query` — optional text search over model names and descriptions, split into whitespace-separated terms; a model matches when every term appears (case-insensitive, any order) in its name or description. Plain substring matching, not semantic search: "finance" will not match a model described only as "revenue".
- `relation_name` — optional *exact* warehouse relation lookup, for "does this object already exist as a public Mesh model?". Supplying it narrows each project to that one relation server-side, before `query` applies. A `query` alongside it can only exclude that single model from `models`; it never broadens the search and has no effect on `lookup`.

Omitting both returns every public model in scope.

**Relation matching** is right-aligned against each model's `database`/`schema`/`identifier`: `fct_orders` matches on identifier alone, `gold.fct_orders` on schema and identifier, `prod.gold.fct_orders` on all three. Every supplied part must match **exactly** — `services_projects_v2` does not match `dim_services_projects_v2` — and matching is **case-insensitive** on every component. Because it matches the warehouse identifier rather than the dbt model name, it finds aliased and versioned models that `query` misses: a v2 model is named `dim_customers` but materializes as `dim_customers_v2`, and looking up that identifier still returns a ref to `dim_customers` with `version=2`. More than three parts, or an empty relation, is an error rather than a silent miss.

**Building the ref:** this tool confirms identity; it does not render the ref for you.
- Only build a ref when `dbt_project_name` is non-null.
- Default form: `{{ ref('<dbt_project_name>', '<model_name>') }}`.
- Add `version=<version>` only when `version` is non-null **and** differs from `latest_version` — omitting it already means "latest".
- `version` is always returned as a string. Render it unquoted (`version=2`) if it looks like a plain integer, otherwise quote it (`version="release-2"`).
- `dependencies.yml` entry: `- name: <dbt_project_name>`, one per producer project — exactly what the top-level `dependencies` field already gives you.

**Returns** a `MeshModelSearchResult`:
- `models` — matching public models, each with `model_name`, `unique_id`, `package_name` (null when the `unique_id` is not `model.<package>.<name>`), `version`, `latest_version`, `project_id`, `relation_name`, `dbt_project_name`, and `warnings`. Pass `project_id` with `unique_id` to `get_node_details` for anything else about a model. A node the platform confirms belongs to a *different* project (an installed package, say) is excluded entirely, since it could never carry a ref for the queried producer; a node whose identity couldn't be resolved is kept with a warning.
- `coverage` — `searched`, `failed` (project → reason), `incomplete` (scan stopped with pages remaining), `unresolved_identity`, and `models_scanned` (`null` on a `relation_name` lookup, which is a point query rather than a scan).
- `lookup` — present only with `relation_name`: the echoed relation, `found`, and `conclusive`.
- `dependencies` — one entry per producer project, deduped, in first-seen order.
- `omitted_model_count` — how many matches `limit` cut.

**Reading `lookup` correctly.** `found: false` is only authoritative when `conclusive: true`. A negative is undermined by any project in `coverage.failed`, `coverage.incomplete`, or `coverage.unresolved_identity`. Two further limits: a *private or protected* model at the relation yields `found: false`, which is correct (it isn't ref-able) but still means the developer is about to source over a dbt-managed relation. And `conclusive: true` means "not claimed by a project in `project_ids`", never "not claimed by any dbt-managed model" — use `get_node_details` for that. Before declaring a new source, make sure every candidate producer is in `project_ids`, then require `conclusive: true`.

**Sizing.** `models` is capped at `limit` (1-100, default 50) across all searched projects — a cap on the merged list, not a GraphQL page, and there is no cursor for more. A nonzero `omitted_model_count` means you should narrow with `query`, `relation_name`, or fewer `project_ids` rather than raise `limit`, which just moves the same cliff. `lookup`, `coverage`, and `dependencies` are all computed over the full scan before truncation, so none of them are affected by the cap. Within a project, results are ordered most-queried first; the merged list concatenates projects in `project_ids` order, so truncation cuts from the end — a project late in `project_ids` can contribute nothing to `models` while still appearing in `dependencies`.

**Examples:**
```python
# Discover candidates across two producer projects
search_public_mesh_models(query="gold finance sales", project_ids=[101, 102])

# Source-to-ref check: omit query for a definitive `lookup` answer
search_public_mesh_models(relation_name="prod.gold.fct_orders", project_ids=[101, 102])
```

**From a result to a ref:** a `models` entry only gives you the pieces — building the string is on you. Given this entry:
```json
{"model_name": "fct_orders", "dbt_project_name": "finance_core", "version": null, "latest_version": null}
```
render:
```sql
{{ ref('finance_core', 'fct_orders') }}
```
A non-null `version` that differs from `latest_version` is added unquoted when it's a plain integer:
```json
{"model_name": "dim_customers", "dbt_project_name": "finance_core", "version": "2", "latest_version": "3"}
```
```sql
{{ ref('finance_core', 'dim_customers', version=2) }}
```
but quoted when it isn't:
```json
{"model_name": "dim_customers", "dbt_project_name": "finance_core", "version": "release-2", "latest_version": "release-3"}
```
```sql
{{ ref('finance_core', 'dim_customers', version="release-2") }}
```
