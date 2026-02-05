# AI SDK Agent

An example of using [Vercel's AI SDK](https://ai-sdk.dev) with dbt MCP.

## Prerequisites

- Node.js 20+
- npm or pnpm

## Configuration

### Remote MCP Server (dbt Cloud)

| Variable | Required | Description |
|----------|----------|-------------|
| `OPENAI_API_KEY` | Yes | OpenAI API key |
| `DBT_TOKEN` | Yes | dbt Cloud API token |
| `DBT_PROD_ENV_ID` | Yes | dbt Cloud production environment ID |
| `DBT_HOST` | No | dbt Cloud host (default: `cloud.getdbt.com`) |

### Local MCP Server

| Variable | Required | Description |
|----------|----------|-------------|
| `OPENAI_API_KEY` | Yes | OpenAI API key |
| `DBT_ENV_FILE` | No | Path to .env file (default: `../../.env`) |

The `.env` file should contain dbt MCP configuration as described in the root README.

## Usage

Install dependencies:

```bash
npm install
```

### Remote (dbt Cloud)

```bash
npm start
```

### Local

```bash
npm run start:local
```

## Example Queries

This is a terminal-based chat interface. After running `npm start` or `npm run start:local`, you can ask natural language questions about your dbt project. Here are some examples:

| Query | Tools Used |
|-------|------------|
| "What version is the MCP server?" | `get_mcp_server_version` |
| "List all available metrics" | `list_metrics` |
| "Show me the dimensions for the revenue metric" | `get_dimensions` |
| "What models are in my project?" | `get_mart_models` |
| "Show the lineage for the orders model" | `get_model_lineage` |
| "Run a query to get total revenue by month" | `query_metrics` |
| "Describe the customers table" | `get_model_columns` |

Type `quit` to exit the chat.

## Example Session

```
Connected to dbt MCP server
Available tools: 65
Type 'quit' to exit

You: What version is the MCP server?
[Tool: get_mcp_server_version]
[Result: {"version":"0.5.0"}]

Assistant: The MCP server version is 0.5.0.

You: List the available metrics
[Tool: list_metrics]
[Result: [{"name":"revenue","type":"simple"},{"name":"orders","type":"simple"}...]]

Assistant: Here are the available metrics in your dbt project...

You: quit
Goodbye!
```

## Alternative LLM Providers

The AI SDK supports multiple providers. To use a different model, modify the import and model in the source files:

```typescript
// Anthropic
import { anthropic } from "@ai-sdk/anthropic";
const model = anthropic("claude-sonnet-4-20250514");

// Google
import { google } from "@ai-sdk/google";
const model = google("gemini-2.0-flash");
```

Install the corresponding provider package (e.g., `@ai-sdk/anthropic`).

## Building Your Own Application

This example is a starting point. The core pattern can be adapted for other use cases:

- **Web API**: Replace the readline loop with an Express/Fastify endpoint
- **Slack Bot**: Handle Slack events and respond with `streamText` results
- **CLI Tool**: Add command-line arguments for non-interactive queries

The key integration points are:

```typescript
// 1. Connect to dbt MCP
const mcpClient = await createMCPClient({ transport });
const tools = await mcpClient.tools();

// 2. Use tools with any AI SDK function
const result = await generateText({ model, tools, prompt });
```
