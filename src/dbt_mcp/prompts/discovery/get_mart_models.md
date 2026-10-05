Get the name and description of all mart models in the environment. A mart model is part of the presentation layer of the dbt project. It's where cleaned, transformed data is organized for consumption by end-users, like analysts, dashboards, or business tools.

Returns one source page in `result`, with `pagination.has_more` and `pagination.next_cursor`. Pass the next cursor as `after` with the same filters. `limit` defaults to 50 and is capped at 100. Local filtering may leave an empty page with more pages available.
