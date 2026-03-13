# Browser-Based Warehouse Authentication Check

## Problem

When dbt is configured to use browser-based warehouse authentication (e.g., Snowflake `authenticator: externalbrowser`), the dbt CLI commands invoked by dbt-mcp fail because:

1. `subprocess.Popen` uses `stdin=subprocess.DEVNULL`, which may block interactive auth flows
2. The default 60-second timeout may be too short for a user to complete browser SSO
3. The user gets no feedback that browser authentication is needed

This affects dbt Core and Fusion users with local `profiles.yml` using browser-based SSO for their data warehouse. dbt Cloud CLI is not affected (warehouse auth is handled server-side).

## Solution

Run `dbt debug` as an eager background task during MCP server lifespan (matching the existing pattern used for LSP connection). This:

1. Triggers the browser auth flow early, before the user invokes any CLI tool
2. Caches the token in the OS keychain (via the dbt adapter)
3. Makes subsequent CLI commands work without re-authentication

### Architecture

```
app_lifespan()
├── register proxied tools (existing)
├── eager LSP connection (existing)
└── NEW: eager warehouse auth check
    └── asyncio.create_task(warehouse_auth_check())
        └── runs `dbt debug` (no stdin=DEVNULL, longer timeout)
            ├── success → auth_status = AUTHENTICATED
            ├── timeout → auth_status = TIMEOUT (with guidance)
            └── error → auth_status = FAILED (with error message)
```

### Auth Status Flow

CLI tools check auth status before executing:
- `AUTHENTICATED` or `NOT_STARTED` (no CLI config): proceed normally
- `IN_PROGRESS`: wait briefly, then proceed (the auth may complete during the command)
- `TIMEOUT` / `FAILED`: return actionable error message to the user

### Key Design Decisions

1. **No profile parsing** — We don't try to detect the auth method. We just run `dbt debug` for all CLI users. It's fast when no browser auth is needed, and triggers the flow when it is.

2. **Non-blocking startup** — The check runs as `asyncio.create_task()`, matching the LSP eager-start pattern. Server starts immediately; the check runs in parallel.

3. **Generous timeout** — The auth check uses a longer timeout (120s) than normal CLI commands (60s) to give users time to complete browser SSO.

4. **No stdin=DEVNULL** — The `dbt debug` subprocess is allowed to interact with the environment so browser auth can work.

## Files to Change

- `src/dbt_mcp/dbt_cli/auth_check.py` (new) — Auth check logic and status tracking
- `src/dbt_mcp/mcp/server.py` — Launch auth check in `app_lifespan()`
- `src/dbt_mcp/dbt_cli/tools.py` — Check auth status before running CLI commands
- `tests/unit/dbt_cli/test_auth_check.py` (new) — Tests
