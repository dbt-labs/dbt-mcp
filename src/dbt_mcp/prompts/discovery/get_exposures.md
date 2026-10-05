Get the id, name, description, and url of all exposures in the dbt environment. Exposures represent downstream applications or analyses that depend on dbt models.

Returns information including:
- uniqueId: The unique identifier for this exposure taht can then be used to get more details about the exposure
- name: The name of the exposure
- description: Description of the exposure
- url: URL associated with the exposure

Returns one source page in `result`, with `pagination.has_more` and `pagination.next_cursor`. Pass the next cursor as `after` with the same filters. `limit` defaults to 50 and is capped at 100. Local filtering may leave an empty page with more pages available.
