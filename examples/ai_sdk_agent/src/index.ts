/**
 * AI SDK Agent Example - Remote (dbt Cloud)
 *
 * Demonstrates connecting to dbt-mcp via dbt Cloud's hosted MCP endpoint.
 * Uses Vercel's AI SDK to wire up an LLM with dbt-mcp tools.
 */

import { experimental_createMCPClient as createMCPClient, streamText } from "ai";
import { openai } from "@ai-sdk/openai";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import * as readline from "readline";

const host = process.env.DBT_HOST ?? "cloud.getdbt.com";
const token = process.env.DBT_TOKEN;
const prodEnvId = process.env.DBT_PROD_ENV_ID;
const accountPrefix = process.env.MULTICELL_ACCOUNT_PREFIX;

if (!token || !prodEnvId) {
  console.error("Missing required environment variables: DBT_TOKEN, DBT_PROD_ENV_ID");
  process.exit(1);
}

async function main() {
  // Connect to dbt Cloud's hosted MCP server
  const baseHost = accountPrefix ? `${accountPrefix}.${host}` : host;
  const transport = new StreamableHTTPClientTransport(
    new URL(`https://${baseHost}/api/ai/v1/mcp/`),
    {
      requestInit: {
        headers: {
          Authorization: `token ${token}`,
          "x-dbt-prod-environment-id": prodEnvId!,
        },
      },
    }
  );

  // Initialize MCP client and fetch available tools
  const mcpClient = await createMCPClient({ transport });
  const tools = await mcpClient.tools();

  console.log("Connected to dbt MCP server");
  console.log(`Available tools: ${Object.keys(tools).length}`);
  console.log("Type 'quit' to exit\n");

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  const prompt = (query: string): Promise<string> =>
    new Promise((resolve) => rl.question(query, resolve));

  try {
    // Main chat loop
    while (true) {
      const userInput = await prompt("You: ");

      if (userInput.toLowerCase() === "quit") {
        break;
      }

      if (!userInput.trim()) {
        continue;
      }

      process.stdout.write("Assistant: ");

      // Stream response from LLM, allowing it to call dbt-mcp tools
      const result = streamText({
        model: openai("gpt-4o"),
        tools,
        maxSteps: 10,
        system: "You are a helpful assistant with access to dbt Cloud tools. Use them to answer questions about the user's dbt project, metrics, models, and data.",
        prompt: userInput,
        onStepFinish: ({ toolCalls, toolResults }) => {
          if (toolCalls?.length) {
            for (const call of toolCalls) {
              console.log(`\n[Tool: ${call.toolName}]`);
            }
          }
          if (toolResults?.length) {
            for (const result of toolResults) {
              const preview = JSON.stringify(result.result).slice(0, 200);
              console.log(`[Result: ${preview}${preview.length >= 200 ? "..." : ""}]`);
            }
            process.stdout.write("\nAssistant: ");
          }
        },
      });

      try {
        for await (const chunk of (await result).textStream) {
          process.stdout.write(chunk);
        }
      } catch (err) {
        console.error(`\n[Error: ${err instanceof Error ? err.message : err}]`);
      }
      console.log("\n");
    }
  } finally {
    rl.close();
    await mcpClient.close();
    console.log("Goodbye!");
  }
}

main().catch(console.error);
