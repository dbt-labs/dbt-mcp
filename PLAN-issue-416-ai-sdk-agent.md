# Implementation Plan: AI SDK Agent Example

**Issue**: [#416 - [Feature]: AI SDK agent example](https://github.com/dbt-labs/dbt-mcp/issues/416)
**Branch**: `claude/plan-issue-416-ev5vj`
**Author**: Claude
**Date**: 2026-02-04

---

## Overview

This plan outlines the implementation of a TypeScript example using [Vercel's AI SDK](https://ai-sdk.dev) to integrate with dbt-mcp. The AI SDK is one of the most popular TypeScript frameworks for building AI applications with 20M+ monthly downloads, making it a valuable addition to the examples directory.

---

## Objectives

1. Create a production-ready TypeScript example demonstrating dbt-mcp integration with the AI SDK
2. Support both local (stdio) and remote (HTTP) MCP server connections
3. Follow existing patterns established by other examples in the repository
4. Maintain professional, clean, and modular code standards

---

## Technical Approach

### Target Framework

**Vercel AI SDK** (`ai` package + `@ai-sdk/mcp`)
- Native MCP support via `createMCPClient`
- Supports multiple LLM providers (OpenAI, Anthropic, Google, etc.)
- TypeScript-first with full type safety
- Streaming support via `streamText`

### Transport Options

| Transport | Use Case | Production Ready |
|-----------|----------|------------------|
| `StreamableHTTPClientTransport` | dbt Cloud hosted MCP | Yes |
| `StdioClientTransport` | Local development | No (dev only) |

---

## File Structure

```
examples/
└── ai_sdk_agent/
    ├── package.json           # Node.js dependencies
    ├── tsconfig.json          # TypeScript configuration
    ├── README.md              # Setup and usage documentation
    └── src/
        ├── index.ts           # Main entry point (HTTP transport)
        ├── local.ts           # Local stdio transport variant
        └── types.ts           # Shared type definitions (if needed)
```

---

## Implementation Details

### 1. Package Configuration (`package.json`)

```json
{
  "name": "ai-sdk-agent",
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "start": "npx tsx src/index.ts",
    "start:local": "npx tsx src/local.ts"
  },
  "dependencies": {
    "ai": "^4.0.0",
    "@ai-sdk/mcp": "^0.5.0",
    "@ai-sdk/openai": "^1.0.0",
    "@modelcontextprotocol/sdk": "^1.12.0"
  },
  "devDependencies": {
    "typescript": "^5.7.0",
    "tsx": "^4.0.0",
    "@types/node": "^22.0.0"
  }
}
```

### 2. TypeScript Configuration (`tsconfig.json`)

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "outDir": "./dist"
  },
  "include": ["src/**/*"]
}
```

### 3. Main Implementation (`src/index.ts`)

**Key Components:**

```typescript
import { createMCPClient } from "@ai-sdk/mcp";
import { streamText } from "ai";
import { openai } from "@ai-sdk/openai";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
```

**Implementation Pattern:**

1. **Environment Configuration**
   - `DBT_TOKEN` - dbt Cloud API token
   - `DBT_HOST` - dbt Cloud host (default: `cloud.getdbt.com`)
   - `DBT_PROD_ENV_ID` - Production environment ID
   - `OPENAI_API_KEY` - LLM provider API key

2. **MCP Client Setup**
   ```typescript
   const transport = new StreamableHTTPClientTransport(
     new URL(`https://${host}/api/ai/v1/mcp/`),
     {
       requestInit: {
         headers: {
           Authorization: `token ${token}`,
           "x-dbt-prod-environment-id": envId,
         },
       },
     }
   );
   const mcpClient = await createMCPClient({ transport });
   ```

3. **Agent Loop**
   - Read user input from stdin
   - Call `streamText` with MCP tools
   - Stream response tokens to stdout
   - Display tool calls as they occur

4. **Resource Cleanup**
   - Close MCP client on exit
   - Handle SIGINT gracefully

### 4. Local Variant (`src/local.ts`)

Uses stdio transport for local dbt-mcp server:

```typescript
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";

const transport = new StdioClientTransport({
  command: "uvx",
  args: ["--env-file", envFilePath, "dbt-mcp"],
});
```

### 5. Documentation (`README.md`)

Structure:
1. **Overview** - Brief description of the example
2. **Prerequisites** - Node.js, npm/pnpm, API keys
3. **Configuration** - Environment variables table
4. **Usage** - Commands for both HTTP and local modes
5. **Customization** - How to change LLM providers

---

## Code Quality Standards

Following the existing patterns in this repository:

1. **No unnecessary comments** - Code should be self-documenting
2. **Minimal dependencies** - Only what's required
3. **Error handling** - Graceful degradation, clear error messages
4. **Type safety** - Full TypeScript strict mode
5. **Consistent style** - Match existing examples' conventions

---

## Testing Strategy

### Manual Testing

1. **HTTP Transport**
   - Set environment variables
   - Run `npm start`
   - Test tool calls (e.g., "list all metrics", "describe model X")
   - Verify streaming output

2. **Local Transport**
   - Configure `.env` file
   - Run `npm run start:local`
   - Verify stdio connection
   - Test same queries as HTTP

### Validation Checklist

- [ ] MCP client connects successfully
- [ ] Tools are discovered from server
- [ ] `streamText` works with tool calls
- [ ] Tool results are incorporated into responses
- [ ] Graceful shutdown on Ctrl+C
- [ ] Clear error messages for missing config

---

## Implementation Phases

### Phase 1: Core Implementation
- Create directory structure
- Implement HTTP transport version (`src/index.ts`)
- Add package.json and tsconfig.json

### Phase 2: Local Development Support
- Implement stdio transport version (`src/local.ts`)
- Add npm scripts for both modes

### Phase 3: Documentation
- Write comprehensive README.md
- Include example conversations
- Document all environment variables

### Phase 4: Polish
- Test both transport modes
- Ensure clean error handling
- Review code for unnecessary comments/complexity

---

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `ai` | ^4.0.0 | Core AI SDK (generateText, streamText) |
| `@ai-sdk/mcp` | ^0.5.0 | MCP client integration |
| `@ai-sdk/openai` | ^1.0.0 | OpenAI provider (swappable) |
| `@modelcontextprotocol/sdk` | ^1.12.0 | MCP transports |
| `typescript` | ^5.7.0 | TypeScript compiler |
| `tsx` | ^4.0.0 | TypeScript execution |

---

## Alternative LLM Providers

The implementation should make it easy to swap providers. The AI SDK supports:

```typescript
// OpenAI
import { openai } from "@ai-sdk/openai";
const model = openai("gpt-4o");

// Anthropic
import { anthropic } from "@ai-sdk/anthropic";
const model = anthropic("claude-sonnet-4-20250514");

// Google
import { google } from "@ai-sdk/google";
const model = google("gemini-2.0-flash");
```

Document this flexibility in the README.

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| AI SDK API changes | Pin specific versions, document tested versions |
| MCP transport compatibility | Test with both dbt Cloud and local server |
| Node.js version requirements | Document minimum Node.js version (20+) |

---

## Success Criteria

1. Working example that connects to dbt-mcp via HTTP
2. Working local development mode via stdio
3. Clean, professional code matching repository standards
4. Comprehensive README with setup instructions
5. Example passes manual testing with real dbt Cloud environment

---

## References

- [AI SDK Documentation](https://ai-sdk.dev)
- [AI SDK MCP Integration](https://ai-sdk.dev/docs/ai-sdk-core/mcp-tools)
- [MCP TypeScript SDK](https://github.com/modelcontextprotocol/typescript-sdk)
- [dbt-mcp Examples](https://github.com/dbt-labs/dbt-mcp/tree/main/examples)

---

## Next Steps

1. Review this plan
2. Approve implementation approach
3. Implement in priority order (Phases 1-4)
4. Submit PR for review
