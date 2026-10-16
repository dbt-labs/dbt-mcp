Get a page of dbt model names and descriptions in the environment.

Returns one source page in `result`, with `pagination.has_more` and `pagination.next_cursor`. Pass the next cursor as `after` with the same filters. `limit` defaults to 50 and is capped at 100. Local filtering may leave an empty page with more pages available.
