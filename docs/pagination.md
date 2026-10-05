# Paginated collection tools

Collection tools return one upstream page per call. Keep filters unchanged when following continuation metadata.

| API | Arguments | Continuation |
| --- | --- | --- |
| Admin | `limit` (default 50, maximum 100), `offset` (default 0) | `pagination.next_offset` |
| Discovery | `limit` (default 50, maximum 100), `after` | `pagination.next_cursor` |
| Semantic Layer | `page_size` (default 50, maximum 100), `page_num` (default 1) | `pagination.next_page` |

Collection responses contain `result` and `pagination`. `list_metrics` retains its formatted metric result and adds pagination alongside it. `pagination.has_more` reports whether another upstream page exists. Post-page filtering can produce an empty result with a continuation.

Metric search lists are evaluated together by the upstream API using OR semantics, so page membership, ordering, and totals come from one combined search. Related metric enrichment preserves the chosen page.

Name resolution inspects at most 100 candidates. An incomplete candidate set requires an explicit unique ID. Invalid page arguments are caller errors; excessive upstream page sizes or a continuation that does not advance are server errors.

These response schemas require coordinated adoption by consumers of the collection tools.
