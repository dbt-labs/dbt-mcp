# dbt MCP Server

This MCP (Model Context Protocol) server provides tools to interact with dbt. Read [this](https://docs.getdbt.com/blog/introducing-dbt-mcp-server) blog to learn more. Add comments or questions to GitHub Issues or join us in [the community Slack](https://www.getdbt.com/community/join-the-community) in the `#tools-dbt-mcp` channel.

## Architecture

![architecture diagram of the dbt MCP server](https://raw.githubusercontent.com/dbt-labs/dbt-mcp/refs/heads/main/docs/d2.png)

## Setup

1. [Install uv](https://docs.astral.sh/uv/getting-started/installation/)
2. Copy the [`.env.example` file](https://github.com/dbt-labs/dbt-mcp/blob/main/.env.example) locally under a file called `.env` and set it with your specific environment variables (see the `Configuration` section of the `README.md`)

## Configuration

The MCP server takes the following environment variable configuration:

### Tool Groups
| Name | Default | Description |
|------|---------|-------------|
| `DISABLE_DBT_CLI` | `false` | Set this to `true` to disable dbt Core, dbt Cloud CLI, and dbt Fusion MCP tools |
| `DISABLE_SEMANTIC_LAYER` | `false` | Set this to `true` to disable dbt Semantic Layer MCP objects |
| `DISABLE_DISCOVERY` | `false` | Set this to `true` to disable dbt Discovery API MCP objects |
| `DISABLE_REMOTE` | `true` | Set this to `false` to enable remote MCP objects |


### Configuration for Discovery, Semantic Layer, and Remote Tools
| Name | Default | Description |
|------|---------|-------------|
| `DBT_HOST` | `cloud.getdbt.com` | Your dbt Cloud instance hostname. This will look like an `Access URL` found [here](https://docs.getdbt.com/docs/cloud/about-cloud/access-regions-ip-addresses). If you are using Multi-cell, do not include the `ACCOUNT_PREFIX` here |
| `MULTICELL_ACCOUNT_PREFIX` | - | If you are using Multi-cell, set this to your `ACCOUNT_PREFIX`. If you are not using Multi-cell, do not set this environment variable. You can learn more [here](https://docs.getdbt.com/docs/cloud/about-cloud/access-regions-ip-addresses) |
| `DBT_TOKEN` | - | Your personal access token or service token. Note: a service token is required when using the Semantic Layer and this service token should have at least `Semantic Layer Only`, `Metadata Only`, and `Developer` permissions. |
| `DBT_PROD_ENV_ID` | - | Your dbt Cloud production environment ID |

### Configuration for Remote Tools
| Name | Description |
|------|-------------|
| `DBT_DEV_ENV_ID` | Your dbt Cloud development environment ID |
| `DBT_USER_ID` | Your dbt Cloud user ID |

### Configuration for dbt CLI
| Name              | Description                                                                                                                                 |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `DBT_PROJECT_DIR` | The path to where the repository of your dbt Project is hosted locally. This should look something like `/Users/firstnamelastname/reponame` |
| `DBT_PATH`        | The path to your dbt Core, dbt Cloud CLI, or dbt Fusion executable. You can find your dbt executable by running `which dbt`                 |

It is also possible to set any environment variable supported by your dbt executable (see [here](https://docs.getdbt.com/reference/global-configs/about-global-configs#available-flags) for the ones supported in dbt Core).

We automatically set `DBT_WARN_ERROR_OPTIONS='{"error": ["NoNodesForSelectionCriteria"]}'` so that the MCP server knows if no node is selected when running a dbt command.
You can overwrite it if needed but we believe that it provides a better experience when calling dbt from the MCP server, making sure that the tool is selecting valid nodes.

## Using with MCP Clients

After going through the [Setup](#setup), you can use dbt-mcp with an MCP client.

Add this configuration to the respective client's config file. Be sure to replace the sections within `<>`:

```json
{
  "mcpServers": {
    "dbt-mcp": {
      "command": "uvx",
      "args": [
        "--env-file",
        "<path-to-.env-file>",
        "dbt-mcp"
      ]
    },
  }
}
```

`<path-to-.env-file>` is where you saved the `.env` file from the Setup step


## Claude Desktop

Follow [these](https://modelcontextprotocol.io/quickstart/user) instructions to create the `claude_desktop_config.json` file and connect.

For debugging, you can find the Claude Desktop logs at `~/Library/Logs/Claude` for Mac or `%APPDATA%\Claude\logs` for Windows.

## Cursor

Note the configuration options [here](#configuration) and input your selections with this link:

<a href="https://cursor.com/install-mcp?name=dbt&config=eyJjb21tYW5kIjoidXZ4IGRidC1tY3AiLCJlbnYiOnsiREJUX0hPU1QiOiJjbG91ZC5nZXRkYnQuY29tIiwiTVVMVElDRUxMX0FDQ09VTlRfUFJFRklYIjoib3B0aW9uYWwtYWNjb3VudC1wcmVmaXgiLCJEQlRfVE9LRU4iOiJ5b3VyLXNlcnZpY2UtdG9rZW4iLCJEQlRfUFJPRF9FTlZfSUQiOiJ5b3VyLXByb2R1Y3Rpb24tZW52aXJvbm1lbnQtaWQiLCJEQlRfREVWX0VOVl9JRCI6InlvdXItZGV2ZWxvcG1lbnQtZW52aXJvbm1lbnQtaWQiLCJEQlRfVVNFUl9JRCI6InlvdXItdXNlci1pZCIsIkRCVF9QUk9KRUNUX0RJUiI6Ii9wYXRoL3RvL3lvdXIvZGJ0L3Byb2plY3QiLCJEQlRfUEFUSCI6Ii9wYXRoL3RvL3lvdXIvZGJ0L2V4ZWN1dGFibGUiLCJESVNBQkxFX0RCVF9DTEkiOiJmYWxzZSIsIkRJU0FCTEVfU0VNQU5USUNfTEFZRVIiOiJmYWxzZSIsIkRJU0FCTEVfRElTQ09WRVJZIjoiZmFsc2UiLCJESVNBQkxFX1JFTU9URSI6ImZhbHNlIn19"><img src="https://cursor.com/deeplink/mcp-install-dark.svg" alt="Add dbt MCP server to Cursor" height="32" /></a>

Cursor MCP docs [here](https://docs.cursor.com/context/model-context-protocol) for reference

## VS Code

1. Open the Settings menu (Command + Comma) and select the correct tab atop the page for your use case
    - `Workspace` - configures the server in the context of your workspace
    - `User` - configures the server in the context of your user
    - **Note for WSL users**: If you're using VS Code with WSL, you'll need to configure WSL-specific settings. Run the **Preferences: Open Remote Settings** command from the Command Palette (F1) or select the **Remote** tab in the Settings editor. Local User settings are reused in WSL but can be overridden with WSL-specific settings. Configuring MCP servers in the local User settings will not work properly in a WSL environment.

2. Select Features → Chat

3. Ensure that "Mcp" is `Enabled`

![mcp-vscode-settings](https://github.com/user-attachments/assets/3d3fa853-2398-422a-8a6d-7f0a97120aba)

4. Click "Edit in settings.json" under "Mcp > Discovery"

5. Add your server configuration (`dbt`) to the provided `settings.json` file as one of the servers:
```json
{
    "mcp": {
        "inputs": [],
        "servers": {
          "dbt": {
            "command": "uvx",
            "args": [
              "--env-file",
              "<path-to-.env-file>",
              "dbt-mcp"
            ]
          },
        }
    }
}
```

`<path-to-.env-file>` is where you saved the `.env` file from the Setup step

6. You can start, stop, and configure your MCP servers by:
- Running the `MCP: List Servers` command from the Command Palette (Control + Command + P) and selecting the server
- Utlizing the keywords inline within the `settings.json` file

![inline-management](https://github.com/user-attachments/assets/d33d4083-5243-4b36-adab-72f12738c263)

VS Code MCP docs [here](https://code.visualstudio.com/docs/copilot/chat/mcp-servers) for reference

## Troubleshooting

- Some MCP clients may be unable to find `uvx` from the JSON config. If this happens, try finding the full path to `uvx` with `which uvx` on Unix systems and placing this full path in the JSON. For instance: `"command": "/the/full/path/to/uvx"`.

## Tools

### dbt CLI

* `build` - Executes models, tests, snapshots, and seeds in dependency order
* `compile` - Generates executable SQL from models, tests, and analyses without running them
* `docs` - Generates documentation for the dbt project
* `ls` (list) - Lists resources in the dbt project, such as models and tests
* `parse` - Parses and validates the project’s files for syntax correctness
* `run` -  Executes models to materialize them in the database
* `test` - Runs tests to validate data and model integrity
* `show` - Runs a query against the data warehouse

> Allowing your client to utilize dbt commands through this MCP tooling could modify your data models, sources, and warehouse objects. Proceed only if you trust the client and understand the potential impact.


### Semantic Layer

* `list_metrics` - Retrieves all defined metrics
* `get_dimensions` - Gets dimensions associated with specified metrics
* `get_entities` - Gets entities associated with specified metrics
* `query_metrics` - Queries metrics with optional grouping, ordering, filtering, and limiting


### Discovery
* `get_mart_models` - Gets all mart models
* `get_all_models` - Gets all models
* `get_model_details` - Gets details for a specific model
* `get_model_parents` - Gets parent nodes of a specific model
* `get_model_children` - Gets children modes of a specific model

### Remote
* `text_to_sql` - Generate SQL from natural language requests
* `execute_sql` - Execute SQL on dbt Cloud's backend infrastructure with support for Semantic Layer SQL syntax. Note: using a PAT instead of a service token for `DBT_TOKEN` is required for this tool.

## Contributing

Read `CONTRIBUTING.md` for instructions on how to get involved!
