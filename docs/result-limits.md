# Result limits and pagination

Admin lists (`list_projects`, `list_jobs`, `list_jobs_runs`), Discovery lists
(`get_all_models`, `get_mart_models`, `get_all_sources`, `get_exposures`,
`get_all_macros`), and Semantic Layer metadata lists return a response containing
`result` and `pagination`. Both are visible in MCP text and structured output.
These changes also apply to the corresponding multi-project tools.

Each call returns one source page. Admin and Discovery accept `limit` (default
50, maximum 100); Semantic Layer metadata accepts `page_size` with the same
bounds. Follow the applicable continuation field:

| API | Request | Continuation |
| --- | --- | --- |
| Admin REST | `limit`, `offset` | `pagination.next_offset` |
| Discovery GraphQL | `limit`, `after` | `pagination.next_cursor` |
| Semantic Layer metadata | `page_size`, `page_num` | `pagination.next_page` |

Keep filters unchanged between pages. Keep `page_size` unchanged when following
Semantic Layer pages; restart at page 1 if you reduce it after a size-limit error.
Local filtering can produce an empty page with `has_more: true`. Macro package
names are unique within a page. Multiple metric search terms are ORed by the
server before pagination, so each metric occurs once and totals are authoritative.
The server must support the optional `searchTerms` metadata argument.
`list_metrics.result` remains CSV text. Other list results retain their row types.

List results are limited to 500 KiB before MCP serialization. An oversized page
fails with an actionable error rather than dropping rows and advancing past them.
Service API bodies have independent 4 MiB transfer and 2 MiB decoded limits,
enforced while streaming, before JSON parsing. Full lineage and resource details
have the same acquisition limits, including SQL and column descriptions. Lineage
retains its existing UI response shape and omitted-node count.

Model detail lookup by name fetches at most 100 alias-filtered candidates in one
request, then checks their names. If more candidates remain, provide `unique_id`
instead; the tool does not return incomplete name resolution.

Artifact calls acquire process-wide capacity before opening the HTTP response:
one active call and up to 128 waiting calls, with a 600-second queue deadline.
Further calls fail before downloading with a retryable capacity error.
Filtered artifacts allow at most 16 MiB transferred and 32 MiB decoded. The
download is spooled to a private temporary file. JSON parsing and jq run in a
separate worker, with incremental output capped at 500 KiB and a 120-second
execution deadline that includes downloading and starts after admission. On Linux the worker also has a
256 MiB address-space limit. Other platforms retain the byte, capacity, deadline,
and output limits, but do not enforce that address-space limit.

Unfiltered artifacts stop at the 500 KiB inline threshold and suggest using jq.
The run-error parser also receives only bounded inline artifacts and processes
steps sequentially (at most 20 steps). Large diagnostics fail with guidance to
inspect a specific artifact and step. Cancellation closes the download, reaps
the worker, removes its temporary file, and releases capacity.

Metadata requests share four active slots and 128 waiting slots per process,
with a 120-second queue deadline. Process-wide gates use thread-safe standard
library semaphores and cancel waiting calls without retaining background waiters.
Operators can override `ACTIVE`, `PENDING`, and `QUEUE_TIMEOUT_SECONDS` using the
`DBT_MCP_API_` and `DBT_MCP_ARTIFACT_` environment prefixes. `Tool admission`
logs identify the gate, configured limits, queue/start/finish/rejection events,
and wait duration. Hosts can observe admission waiting to budget queue time
separately from execution time.

Invalid arguments and oversized final pages include corrective guidance.
Response decoding faults, upstream page-contract violations, and acquisition
limits are server errors; capacity errors indicate temporary overload.
Product-document downloads and their cache are bounded independently. Cached page
URLs count toward the cache's byte budget alongside their content. Local CLI,
codegen, and dev-lineage tools retain their existing behavior. Query execution
limits are separate from metadata pagination.
