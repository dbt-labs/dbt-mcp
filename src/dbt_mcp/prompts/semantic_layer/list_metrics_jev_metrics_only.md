List metrics from the dbt Semantic Layer.

If the user is asking a data-related or business-related question, use this tool as a first step, and pass their question to the `question` parameter.

**Pass `question` verbatim.** Give the user's question exactly as they asked it, in their own words. Do not paraphrase it, shorten it, or rewrite it into metric or column names — the ranking works on natural business vocabulary, and rewriting it into technical terms makes the results worse.

When `question` is provided, the response contains:

1. A `# Ranked by relevance...` note giving how many metrics were returned out of how many exist.
2. A CSV of the best-matching metrics with a `relevance` column (0-1) and full `description` values. Read the descriptions — catalogs routinely contain near-duplicate metrics (for example a total, an enterprise-only variant, and a self-serve-only variant) whose names look similar and whose descriptions are the only thing distinguishing them. Pick using the description, not the name.

**This response does not include dimensions.** Once you've picked the metric(s) you need, call `get_dimensions` yourself to see what you can group or filter by.

Relevance scores are a guide, not a decision. If every score is low, no metric matches the question well — say so rather than forcing a choice. If several metrics score closely, they may all be needed, or the question may be ambiguous; ask the user rather than guessing.

`search` filters by substring on the metric name and may be combined with `question`, in which case substring filtering happens first and ranking happens within the survivors. If `search` matches nothing and `question` is set, the full catalog is ranked instead and a `# Note:` line says so.

`meta_filter` accepts a dict of key-value pairs and restricts results to metrics whose `config.meta` contains all specified pairs — for example `{"agent_accessible": true}`.

Without `question`, this tool behaves as a plain listing: a CSV with a header row and dynamic columns (`name` and `type` always present; `label`, `description`, `metadata`, `dimensions`, `entities` only when at least one metric has a value). Broad listings that exceed the size budget drop `description` first and then `metadata`, prefixed with a `# Note:` line. `metric_time` is a standard time dimension available on most metrics.
