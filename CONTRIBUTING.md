# Contributing

With [task](https://taskfile.dev/) installed, simply run `task` to see the list of available commands. For comments, questions, or requests open a GitHub issue.

## Setup

1. Clone the repository:
```shell
git clone https://github.com/dbt-labs/dbt-mcp.git
cd dbt-mcp
```

2. [Install uv](https://docs.astral.sh/uv/getting-started/installation/)

3. [Install Task](https://taskfile.dev/installation/)

4. Run `task install`

5. Configure environment variables:
    ```shell
    cp .env.example .env
    ```
    Then edit `.env` with your specific environment variables (see [our docs](https://docs.getdbt.com/docs/dbt-ai/setup-local-mcp) to determine the values).

6. Run `task client` to chat with dbt MCP in your terminal.

## Testing

This repo has automated tests which can be run with `task test:unit`. Additionally, there is a simple CLI tool which can be used to test by running `task client`. If you would like to test in a client like Cursor or Claude, use a configuration file like this:

```
{
  "mcpServers": {
    "dbt": {
      "command": "<path-to-uv>",
      "args": [
        "--directory",
        "<path-to-this-directory>/dbt-mcp",
        "run",
        "dbt-mcp",
        "--env-file",
        "<path-to-this-directory>/dbt-mcp/.env"
      ]
    }
  }
}
```

Or, if you would like to test with Oauth, use a configuration like this:

```
{
  "mcpServers": {
    "dbt": {
      "command": "<path-to-uv>",
      "args": [
        "--directory",
        "<path-to-this-directory>/dbt-mcp",
        "run",
        "dbt-mcp",
      ],
      "env": {
        "DBT_HOST": "<dbt-host-with-custom-subdomain>",
      }
    }
  }
}
```

For improved debugging, you can set the `DBT_MCP_SERVER_FILE_LOGGING=true` environment variable to log to a `./dbt-mcp.log` file.

## Signed Commits

Before committing changes, ensure that you have set up [signed commits](https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits).
This repo requires signing on all commits for PRs.

## Changelog

Every PR requires a changelog entry. [Install changie](https://changie.dev/) and run `changie new` to create a new changelog entry.

## Debugging

The dbt-mcp server runs with `stdio` transport by default which does not allow for Python debugger support. For debugging with breakpoints, use `streamable-http` transport.

### Option 1: MCP Inspector Only (No Breakpoints)
1. Run `task inspector` - this starts both the server and inspector automatically
2. Open MCP Inspector UI
3. Use "STDIO" Transport Type to connect
4. Test tools interactively in the inspector UI (uses `stdio` transport, no debugger support)

### Option 2: VS Code Debugger with Breakpoints (Recommended for Debugging)
1. Set breakpoints in your code
2. Press `F5` or select "debug dbt-mcp" from the Run menu
3. Open MCP Inspector UI via `npx @modelcontextprotocol/inspector`
4. Connect to `http://localhost:8000/mcp/v1` using "Streamable HTTP" transport and "Via Proxy" connection type
5. Call tools from Inspector - your breakpoints will trigger

### Option 3: Manual Debugging with `task dev`
1. Run `task dev` - this starts the server with `streamable-http` transport on `http://localhost:8000`
2. Set breakpoints in your code
3. Attach your debugger manually (see [debugpy documentation](https://github.com/microsoft/debugpy#debugpy) for examples)
4. Open MCP Inspector via `npx @modelcontextprotocol/inspector`
5. Connect to `http://localhost:8000/mcp/v1` using "Streamable HTTP" transport and "Via Proxy" connection type
6. Call tools from Inspector - your breakpoints will trigger

**Note:** `task dev` uses `streamable-http` by default. The `streamable-http` transport allows the debugger and MCP Inspector to work simultaneously without conflicts. To override, use `MCP_TRANSPORT=stdio task dev`.

If you encounter any problems, you can try running `task run` to see errors in your terminal.

## Release

Only people in the `CODEOWNERS` file should trigger a new release with these steps:

1. Consider these guidelines when choosing a version number:
  - Major
    - Removing a tool or toolset
    - Changing the behavior of existing environment variables or configurations
  - Minor
    - Changes to config system related to the function signature of the register functions (e.g. `register_discovery_tools`)
    - Adding optional parameters to a tool function signature
    - Adding a new tool or toolset
    - Removing or adding non-optional parameters from tool function signatures
  - Patch
    - Bug and security fixes - only major security and bug fixes will be back-ported to prior minor and major versions
    - Dependency updates which don’t change behavior
    - Minor enhancements
    - Editing a tool or parameter description prompt
    - Adding an allowed environment variable with the `DBT_MCP_` prefix
2. Trigger the [Create release PR Action](https://github.com/dbt-labs/dbt-mcp/actions/workflows/create-release-pr.yml).
  - If the release is NOT a pre-release, select `auto` (default) to automatically determine the version bump based on changelog entries, or manually pick patch, minor or major if needed
  - If the release is a pre-release, set the bump and the pre-release suffix. We support alpha.N, beta.N and rc.N.
    - use alpha for early releases of experimental features that specific people might want to test. Significant changes can be expected between alpha and the official release.
    - use beta for releases that are mostly stable but still in development. It can be used to gather feedback from a group of peopleon how a specific feature should work.
    - use rc for releases that are mostly stable and already feature complete. Only bugfixes and minor changes are expected between rc and the official release.
  - Picking the prerelease suffix will depend on whether the last release was the stable release or a pre-release:

| Last Stable | Last Pre-release | Bump  | Pre-release Suffix | Resulting Version |
| ----------- | ---------------- | ----- | ------------------ | ----------------- |
| 1.2.0       | -                | minor | beta.1             | 1.3.0-beta.1      |
| 1.2.0       | 1.3.0-beta.1     | minor | beta.2             | 1.3.0-beta.2      |
| 1.2.0       | 1.3.0-beta.2     | minor | rc.1               | 1.3.0-rc.1        |
| 1.2.0       | 1.3.0-rc.1       | minor |                    | 1.3.0             |
| 1.2.0       | 1.3.0-beta.2     | minor | -                  | 1.3.0             |
| 1.2.0       | -                | major | rc.1               | 2.0.0-rc.1        |
| 1.2.0       | 2.0.0-rc.1       | major | -                  | 2.0.0             |

3. Get this PR approved & merged in (if the resulting release name is not the one expected in the PR, just close the PR and try again step 1)
4. This will trigger the `Release dbt-mcp` Action. On the `Summary` page of this Action a member of the `CODEOWNERS` file will have to manually approve the release. The rest of the release process is automated.
