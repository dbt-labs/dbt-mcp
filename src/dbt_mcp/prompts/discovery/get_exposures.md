Get the id, name, description, and url of all exposures in the dbt environment. Exposures represent downstream applications or analyses that depend on dbt models.

Returns information including:
- uniqueId: The unique identifier for this exposure taht can then be used to get more details about the exposure
- name: The name of the exposure
- description: Description of the exposure
- url: URL associated with the exposure