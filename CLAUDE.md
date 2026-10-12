# CLAUDE.md

## Project Overview

dbt-mcp is an MCP (Model Context Protocol) server that exposes dbt functionality as tools to AI assistants. Built on `FastMCP` from the `mcp` SDK.

## Key Paths

- Entry point: `src/dbt_mcp/main.py`
- Server: `src/dbt_mcp/mcp/server.py` (`DbtMCP` class, `create_dbt_mcp()`)
- Tool infra: `src/dbt_mcp/tools/` (definitions, registration, injection, toolsets, tool_names)
- Tool categories: `discovery/`, `semantic_layer/`, `dbt_cli/`, `dbt_codegen/`, `dbt_admin/`, `lsp/`, `mcp_server_metadata/`
- Prompts (tool descriptions): `src/dbt_mcp/prompts/`
- Config: `src/dbt_mcp/config/`
- Tests: `tests/unit/`, `tests/integration/`

## Tool Architecture

Tools follow a consistent pattern:
1. `@dbt_mcp_tool` decorator defines the tool with metadata
2. `ToolName` enum in `tools/tool_names.py` — every tool needs an entry
3. Toolset mapping in `tools/toolsets.py` — maps tools to categories
4. Context injection via `adapt_context()` — tools receive typed context objects, but MCP only sees user-facing params
5. `register_tools()` in `tools/register.py` — precedence-based enablement (individual > toolset > default)

### MCP Apps (tools with interactive UI)

Tools can have associated UIs via the `meta` field:
- `meta={"ui": {"resourceUri": "ui://dbt-mcp/app-name"}}` on `@dbt_mcp_tool`
- `structured_output=True` required so the host can pass structured JSON to the UI
- Return type should be a Pydantic model
- Register matching resource with `@dbt_mcp.resource(uri=..., mime_type="text/html;profile=mcp-app")`
- Frontend uses `@modelcontextprotocol/ext-apps` SDK

## Commands

- `task test:unit` — run unit tests
- `task test:integration` — run integration tests (requires dbt Platform credentials)
- `task install` — install dependencies
- `task check` — run linting and type checking. **Run before every PR push.**
- `task fmt` — format code
- `task docs:generate` — regenerate README tool list and d2 diagram from tool definitions (run after adding/removing tools)
- `task dev` — run server with streamable-http transport
- `task inspector` — run with MCP Inspector
- `uv run pytest tests/ --ignore=tests/integration -x -q` — quick unit test run

## PRs

- **This is an open-source repo.** Do not include internal links (Notion, Slack, Jira) or internal details in PR descriptions, commit messages, or code comments. Keep PR descriptions focused on the public-facing what and why.
- Run `task check` before every PR push
- Run `task test:unit` before every commit to catch failures early

## Style

- See `CONTRIBUTING.md` for Python conventions
- Import at top of file, type annotations on all functions
- Prefer Pydantic models or dataclasses over dicts
- Use `*` in param lists when adjacent params share a type
- Avoid code in `__init__.py`

## Dependencies

- Declare every third-party package your code imports directly. Packages imported by `src/dbt_mcp` or `src/remote_mcp` go in `dependencies`; packages imported only by tests, evals or scripts go in the `dev` group. Never rely on a package that is only installed because another dependency requires it: a change to what that dependency requires then removes or downgrades it, and the first sign is an `ImportError` at runtime.
- Declare a new import's package in the same change as the import, with `uv add`. Use the version in `uv.lock` as the lower bound.
- Using a private module or attribute of a third-party package (a name that starts with `_`, such as `pydantic._internal` or `mcp.shared._httpx_utils`) requires an exact pin of that package in `pyproject.toml` (`==`), with a comment naming the private API used and what to re-check when bumping the version, like the `mcp` pin. Prefer a public API where one exists.
- To check, run `uv run --frozen --no-sync --with deptry deptry src tests scripts evals --known-first-party dbt_mcp --known-first-party remote_mcp` and look for `DEP003` (transitive) findings.

## Changelog

Every PR requires a changelog entry. Run `changie new --kind "<kind>" --body "<description>"` to create one.
Valid kinds: `Breaking Change` (major), `Enhancement or New Feature` (minor), `Under the Hood` (patch), `Bug Fix` (patch), `Security` (patch). See `CONTRIBUTING.md` for full contributing guidelines.

## Testing

- `MockFastMCP` in `tests/conftest.py` captures registered tools and their kwargs (including `meta`)
- Tool definition tests: `tests/unit/tools/test_definitions.py`
- Precedence logic tests: `tests/unit/tools/test_precedence.py`
