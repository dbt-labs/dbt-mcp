The node to describe. Accepted spellings:

- A node name, such as `orders` or `country_codes`.
- A method selector, such as `source:raw.payments`, `exposure:revenue_dashboard`, `metric:revenue`, `semantic_model:orders`, or `saved_query:orders_query`.
- A unit-test selector: a name or FQN intersected with `test_type:unit`, such as `test_orders,test_type:unit`.
- A dbt unique_id, such as `model.my_project.orders`, `seed.my_project.country_codes`, `snapshot.my_project.orders_snapshot`, `source.my_project.raw.payments`, or `exposure.my_project.revenue_dashboard`. Unique IDs are resolved to a `dbt list` selector using the local manifest.

Names and method selectors are passed to `dbt list --select` unchanged.
