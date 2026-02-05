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
