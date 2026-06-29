# Deprecating a tool

Tools in dbt-mcp follow a **deprecate-then-remove** lifecycle rather than being
deleted outright. This document explains why and how.

## Why tools can't just be deleted

The dbt MCP server is a **published app** in the Anthropic and OpenAI app stores.
When an app is submitted, the host caches the server's tool surface — names, titles,
descriptions, input/output schemas, annotations, and `_meta` — as a versioned
contract at submission time. Removing or renaming a tool is a breaking change for
all installed clients that depend on that tool string.

A deploying the server immediately removes a tool from the live server, but the
published contract (the snapshot submitted to the app store) still lists it. Clients
using the published version see a mismatch; the tool disappears without warning.
See `CONTRIBUTING.md` (§ Published-app contract) and
`src/dbt_mcp/contract/snapshot.py` for technical details.

## The deprecate-then-remove lifecycle

1. **Add the replacement tool.** Ship the new, consolidated tool. Its contract
   snapshot is submitted with the next app-store update.
2. **Deprecate the old tool.** Keep it registered and callable. Apply a deprecation
   banner to its description and a `deprecated` / `replacement` signal to its `meta`.
   Publish a compatible app update.
3. **Monitor usage.** Track calls to the deprecated tool name via the `ToolCalled`
   telemetry event (see [Monitoring](#monitoring-usage-before-removal)). Wait until
   usage falls to approximately zero over a trailing 30-day window.
4. **Remove the tool.** Delete it in a separate PR, bump the major version, and
   submit a breaking-change app update.

## How to deprecate a tool

Use the helpers in `src/dbt_mcp/tools/deprecation.py`:

```python
from dbt_mcp.tools.deprecation import deprecated_description, deprecation_meta

@dbt_mcp_tool(
    description=deprecated_description(
        get_prompt("discovery/get_model_parents"), replacement="get_lineage"
    ),
    meta=deprecation_meta(replacement="get_lineage"),
    title="Get Model Parents",
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
)
async def get_model_parents(...):
    ...
```

`deprecated_description` prepends a standard banner:

> **DEPRECATED — use \`get_lineage\` instead.** This tool will be removed in a
> future release.

`deprecation_meta` returns `{"deprecated": True, "replacement": "<name>"}`, which
surfaces as `Tool._meta` in the MCP protocol so clients can inspect it
programmatically.

### What to change

- Apply `deprecated_description(...)` + `deprecation_meta(replacement="<new>")` at
  the `@dbt_mcp_tool(...)` call site in **both** `discovery/tools.py` and
  `discovery/tools_multiproject.py`.

### What NOT to change

Do **not** delete anything during deprecation — the tool must stay registered and
callable. Keep all of the following intact:

- `ToolName` enum entry in `tools/tool_names.py`
- Toolset mapping in `tools/toolsets.py`
- Human description in `tools/readme_mappings.py`
- Entry in `DISCOVERY_TOOLS` / `MULTIPROJECT_DISCOVERY_TOOLS`
- Prompt file in `prompts/discovery/`
- Client fetch code (GraphQL query, fetcher method)

### Checklist

- [ ] Apply `deprecated_description` + `deprecation_meta` in both `tools.py` files
- [ ] `changie new --kind "Enhancement or New Feature" --body "Deprecated <tool>; use <replacement> instead."`
- [ ] `task docs:generate`
- [ ] `task contract:generate` — regenerates `tests/unit/contract/contract_snapshot.json`; commit the result
- [ ] `task check` + `task test:unit`
- [ ] Submit a new app version after merge (the snapshot changed)

## Monitoring usage before removal

Every tool call emits a `ToolCalled` telemetry event keyed on the field `tool_name`
(the registered tool string, e.g. `"get_model_parents"`), with feature tag
`dbt-mcp`. The event is emitted in `src/dbt_mcp/mcp/server.py` (`DbtMCP.call_tool`)
and flows through `src/dbt_mcp/tracking/tracking.py` (`emit_tool_called_event`).
Proxied tools are filtered out before emission — only tools directly served by this
repo are tracked.

Query `ToolCalled` events filtered to the deprecated `tool_name` over a trailing
30-day window. The bar for proceeding to removal is approximately zero calls.

## Removing a tool (Phase B)

Once usage is ~0, remove the tool in a dedicated PR:

1. Delete the tool function from `discovery/tools.py` and `tools_multiproject.py`,
   and its entry in `DISCOVERY_TOOLS` / `MULTIPROJECT_DISCOVERY_TOOLS`.
2. Delete from `tools/tool_names.py`, `tools/toolsets.py`,
   `tools/readme_mappings.py`.
3. Delete the prompt file from `prompts/discovery/`.
4. Delete the `deprecated_description` / `deprecation_meta` call sites added in the
   deprecation PR.
5. Delete now-unused client code (grep-confirm no other callers first).
6. Update the round-trip tests (`test_tool_names.py` / `test_toolsets.py`) to remove
   the tool from the expected set.
7. `changie new --kind "Breaking Change" --body "Removed <tool>."`
8. `task docs:generate`
9. `task contract:generate` — commit the updated snapshot
10. `task check` + `task test:unit`
11. Submit a new app version after merge.
