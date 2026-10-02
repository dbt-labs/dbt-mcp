---
name: code-review
description: Review dbt-mcp pull requests for semantic regressions and missing test scenarios across tool descriptions, configuration, published clients, and MCP Apps.
---

# Review dbt-mcp changes

Compare the pull request's stated intent with the behavior agents and existing clients will observe. Follow the affected paths through tool descriptions, configuration, backend calls, and consumers.

## Review semantic boundaries

- **Tool intent:** Compare prompts in `src/dbt_mcp/prompts/` with the backend operation, parameter defaults, and returned data. Check whether an agent following the description would choose the right tool and arguments, and whether read-only, destructive, and idempotent annotations still describe the actual side effects.
- **Configuration and context:** Trace affected calls through context injection and `src/dbt_mcp/tools/register.py`. Consider conflicting individual/toolset settings and the distinction between an unset enable list and an explicitly empty one. For tools supporting multiple projects, check that each requested project is selected and that calls use its resolved environment and current credentials.
- **Published clients:** Hosts retain tool metadata cached at app publication. Consider whether existing calls still mean the same thing after deployment, especially when defaults, result interpretation, or side effects change without a schema change. See `src/dbt_mcp/contract/snapshot.py` for the snapshot's scope and exclusions.
- **MCP Apps:** Assess the meaning of changed structured results, such as lineage edge direction and omitted-node counts. Follow the results to the consumer of the linked `ui://` resource when its source is available. UI resource content is outside the contract snapshot; identify any consumer behavior that cannot be verified from this repository.

## Review missing test scenarios

- Look for a concrete caller or configuration that exercises the changed behavior but is absent from the tests, such as a conflicting tool enablement setting or a backend error interpreted as an empty success.
- Assess whether assertions would catch the behavioral regression: a mocked backend returning the expected fixture may leave argument selection, context binding, or error translation untested. Tie a proposed test to the affected path and its observable outcome.
