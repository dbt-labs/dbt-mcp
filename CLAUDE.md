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
1. `@dbt_mcp_tool` defines metadata and requires an explicit access declaration: target argument annotations, top-level `requirements=(AccountTarget(requires=…),)` or a host policy enum. Use `requirements=(AccessPolicy.LOCAL,)` for local execution and `requirements=()` for public tools. Hosts implement enforcement; shared declarations contain semantic permissions and preserve host policies without interpreting them.
2. `ToolName` enum in `tools/tool_names.py` — every tool needs an entry
3. Toolset mapping in `tools/toolsets.py` — maps tools to categories
4. Context injection via `adapt_context()` — tools receive typed context objects, but MCP only sees user-facing params
   - Declare `Annotated[..., ProjectTarget(...)]` and `EnvironmentTarget(...)` on the canonical tool arguments. The mapper receives resolved values, and the tool function receives those same arguments. Adaptation preserves the canonical metadata. `JobTarget`/`RunTarget` describe IDs whose owning project/environment the host resolves.
   - Discovery, semantic-layer and job-listing tools declare `ProjectTarget(requires=..., environment=EnvironmentRole.PRODUCTION)`. The host resolves production alongside the project before authorization; no environment-ID argument is needed. Tool bodies consume the configuration supplied by their context mapper.
   - `adapt_with_mappers(context=build_context, project_id=resolved_project)` injects parameters by name. Mapper inputs form the exposed signature, shared inputs must agree, and each mapper runs once per invocation. Target annotations remain on caller-visible inputs. `adapt_context()` is a convenience for type-based context injection and accepts additional named parameter mappers.
   - Config adapter factories choose their mappers once for project-aware or selected configuration; registration adapts each definition once. Admin registration requires a production config provider. `bind_schema()` projects request-specific MCP schemas; hosts enforce the same bindings on invocation. Adaptation never modifies canonical definitions.
   - Discovery and semantic-layer tools have one canonical implementation and one registration entry point. Project-aware providers resolve either the configured environment or an explicit project. The local server uses one registry and projects schemas per credential selection: a unique project/configured environment hides the selector; multiple projects expose a required integer selector. Invocation rejects supplied bound selectors. Optional job filters can declare `JobTarget(..., when_missing=AccountTarget(...))`; shared access policies contain only local execution. `list_jobs` always lists the production environment bound by its context, with no account-wide fallback or project-wide override.
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

## Changelog

Every PR requires a changelog entry. Run `changie new --kind "<kind>" --body "<description>"` to create one.
Valid kinds: `Breaking Change` (major), `Enhancement or New Feature` (minor), `Under the Hood` (patch), `Bug Fix` (patch), `Security` (patch). See `CONTRIBUTING.md` for full contributing guidelines.

## Testing

- `MockFastMCP` in `tests/conftest.py` captures registered tools and their kwargs (including `meta`)
- Tool definition tests: `tests/unit/tools/test_definitions.py`
- Precedence logic tests: `tests/unit/tools/test_precedence.py`
