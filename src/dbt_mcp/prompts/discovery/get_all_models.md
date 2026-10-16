Get a page of dbt model names and descriptions in the environment.

Optionally pass `access` to return only models at one dbt Mesh access level (`private`/`protected`/`public`) — use `access="public"` to audit the interface this project exposes to dbt Mesh consumers. Each item carries `name`, `uniqueId`, and `description`; call `get_node_details` for a model's `access`, `group`, `contractEnforced`, or any other config.

Returns one source page in `result`, with `pagination.has_more` and `pagination.next_cursor`. Pass the next cursor as `after` with the same filters. `limit` defaults to 50 and is capped at 100. Local filtering may leave an empty page with more pages available.
