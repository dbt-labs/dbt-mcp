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

## Style

- See `.cursor/rules/python.mdc` for Python conventions
- Import at top of file, type annotations on all functions
- Prefer Pydantic models or dataclasses over dicts
- Use `*` in param lists when adjacent params share a type
- Avoid code in `__init__.py`

## Changelog

Every PR requires a changelog entry. Run `changie new --kind "<kind>" --body "<description>"` to create one.
Valid kinds: `Breaking Change` (major), `Enhancement or New Feature` (minor), `Under the Hood` (patch), `Bug Fix` (patch), `Security` (patch). See `CONTRIBUTING.md` for full contributing guidelines.

## Testing

- `MockFastMCP` in `tests/conftest.py` captures registered tools and their kwargs (including `meta`)
- Tool definition tests: `tests/unit/tools/test_definitions.py`
- Precedence logic tests: `tests/unit/tools/test_precedence.py`
