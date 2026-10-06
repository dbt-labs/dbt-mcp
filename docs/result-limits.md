# Response and artifact memory limits

Service API bodies have independent 4 MiB transfer and 2 MiB decoded limits,
enforced while streaming before JSON parsing. Full lineage and resource details
have the same acquisition limits, including SQL and column descriptions. Lineage
retains its existing UI response shape and omitted-node count.

Admin collections, Discovery collections, and Semantic Layer metadata use
native pagination and the streaming acquisition limits above. Results are not
re-serialized to enforce a separate generic output-byte limit. Environment
resolution fetches active environments in pages and rejects upstream responses
that exceed the requested page size; it has no separate total environment cap.

Pagination arguments, continuation metadata, and combined metric search were
added in [the pagination PR](https://github.com/dbt-labs/dbt-mcp/pull/915).
These memory limits preserve its collection tool schemas and native continuations.

Artifact calls enter a supplied admission context before opening the HTTP response.
The host chooses process-wide capacity and queue policy. By default the library
does not impose a server queue. Filtered artifacts default to at most 16 MiB
transferred and 32 MiB decoded. The
download is spooled to a private temporary file. JSON parsing and jq run in a
separate worker, with incremental output capped at 500 KiB and a 120-second
execution deadline that includes downloading and starts after admission. On Linux the worker also has a
256 MiB address-space limit. Other platforms retain the byte, capacity, deadline,
and output limits, but do not enforce that address-space limit.

Unfiltered artifacts stop at the 500 KiB inline threshold and suggest using jq.
The run-error parser also receives only bounded inline artifacts and processes
steps sequentially (at most 20 steps). Cancellation closes the download, reaps
the worker, removes its temporary file, and releases capacity.

Hosts inject immutable budgets and admission factories through configuration:

- `AdminApiConfig.http_config`, `DiscoveryConfig.http_config`, and
  `SemanticLayerConfig.http_config` accept `HttpConfig`, containing response-byte
  budgets and an async context-manager factory for admission.
- `AdminApiConfig.artifact_config` accepts `ArtifactConfig`, containing source,
  inline/output, worker-memory, filter-length, and execution budgets plus its own
  admission factory. That context remains held until download, evaluation, child
  reaping, and temporary-file cleanup finish.
- `ProductDocsClient` accepts `http_config` and `ProductDocsConfig` for its
  full-text index and cache budgets.

Reuse host-owned admission factories across client configurations to share
capacity across requests. The library has no service-specific gate instances,
environment overrides, shutdown policy, or queue-wait accounting. These hooks
are configuration dependencies, not published tool arguments; tool schemas and
pagination remain unchanged. The budgets above are portable per-call defaults,
which hosts can replace independently of their admission policy.

Invalid arguments and oversized inline or filtered artifacts include corrective guidance.
Response decoding faults, upstream page-contract violations, and acquisition
limits are server errors; capacity errors indicate temporary overload.
Product-document downloads and their cache are bounded independently. Cached page
URLs count toward the cache's byte budget alongside their content. Local CLI,
codegen, and dev-lineage tools retain their existing behavior. Query execution
limits are separate from metadata pagination.
