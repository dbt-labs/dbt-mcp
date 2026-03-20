# Testing Code Mode on a Real dbt Project in Cursor

This guide walks through testing the Code Mode feature (token-efficient tool surface) with a real dbt project in Cursor.

## 1. Enable Code Mode and point at your dbt project

Code Mode is enabled with **`DBT_MCP_ENABLE_CODE_MODE=true`**. The server then exposes only two tools—`codemode_search` and `codemode_execute`—instead of the full tool list, to reduce token usage.

Configure Cursor’s MCP so the dbt MCP server gets the right env (including your project path and Code Mode).

**Option A: Project-specific config (recommended)**

Create or edit **`.cursor/mcp.json`** in your **dbt project** (the repo you work in, not the dbt-mcp repo):

```json
{
  "mcpServers": {
    "dbt": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/your/dbt-mcp/repo",
        "run",
        "dbt-mcp"
      ],
      "env": {
        "DBT_PROJECT_DIR": "/path/to/this/dbt/project",
        "DBT_PATH": "dbt",
        "DBT_MCP_ENABLE_CODE_MODE": "true"
      }
    }
  }
}
```

- Replace `/path/to/your/dbt-mcp/repo` with the path to your **dbt-mcp** clone (so `uv run dbt-mcp` runs from there).
- Replace `/path/to/this/dbt/project` with the path to the **dbt project** you want to test (the one containing `dbt_project.yml`). If you’re editing `.cursor/mcp.json` inside that project, you can use `"${workspaceFolder}"` if your client supports it, or an absolute path.
- `DBT_PATH`: use `dbt` if `dbt` is on your PATH, or the full path to the dbt executable.

**Option B: Use an env file**

Prefer loading the file with **`uv run`** (so variables are in the process before Python starts):

```json
{
  "mcpServers": {
    "dbt": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/dbt-mcp",
        "run",
        "--env-file",
        "/path/to/your/mcp.env",
        "dbt-mcp"
      ]
    }
  }
}
```

You can also use **`dbt-mcp --env-file /path/to/mcp.env`** after `run`; `main()` loads that file before reading settings (so `DBT_MCP_ENABLE_CODE_MODE` in that file applies).

Add **`DBT_MCP_ENABLE_CODE_MODE=true`** and **`DBT_PROJECT_DIR`** in that env file so the server sees your real project and Code Mode.

**Option C: Global Cursor config**

Use **`~/.cursor/mcp.json`** (macOS/Linux) or **`%USERPROFILE%\.cursor\mcp.json`** (Windows) and set the same `command`, `args`, and `env` there. Less ideal if you switch between multiple dbt projects.

## 2. Restart Cursor

After changing `mcp.json`, fully quit and reopen Cursor so it restarts the MCP server with the new env.

## 3. Confirm the server is in Code Mode

In Cursor, open the MCP / tools UI and check that the dbt server only lists:

- **codemode_search**
- **codemode_execute**

If you see the full list (e.g. `list_models`, `get_lineage`, …), Code Mode is off. Double-check:

- `DBT_MCP_ENABLE_CODE_MODE` is set to `"true"` in the `env` (or in the `.env` file passed via `--env-file`).
- You restarted Cursor after editing the config.

## 4. Try it in chat

With Code Mode on, the model should use the two code-mode tools instead of individual tools.

**Search (discover tools without loading full schemas)**

- “Which dbt tools are related to models or lineage?”
- “List tools that mention metrics or semantic layer.”

The assistant should call **codemode_search** with Python that filters the `catalog` (e.g. by name/description).

**Execute (run multiple tools in one go)**

- “List my mart models and then show details for the first one.”
- “Get all models, then filter to those with ‘stg’ in the name and return their names.”

The assistant should call **codemode_execute** with async Python that uses `await dbt.<tool_name>(...)` (e.g. `dbt.get_mart_models()`, `dbt.get_model_details(...)`).

## 5. Optional: dbt Cloud / Platform

If you use dbt Cloud and want Discovery, Semantic Layer, etc., in Code Mode, add the usual env vars (e.g. in `env` or `.env`):

- `DBT_HOST`
- `DBT_PROD_ENV_ID`
- `DBT_TOKEN`
- (and any other vars your setup needs)

Code Mode only changes *how* tools are exposed (search + execute); it doesn’t disable Platform features.

## 6. Debugging

- **Logs**: Set `DBT_MCP_SERVER_FILE_LOGGING=true` and optional `DBT_MCP_LOG_LEVEL=DEBUG` in `env` (or `.env`). Check the log path mentioned in the dbt-mcp docs (e.g. `dbt-mcp.log` in the server directory).
- **Wrong project**: Ensure `DBT_PROJECT_DIR` is the directory that contains `dbt_project.yml` for the project you expect.
- **Code mode not on**: Ensure no other config (e.g. a different `mcp.json` or env file) overrides `DBT_MCP_ENABLE_CODE_MODE`, and that you restarted Cursor after changes.
