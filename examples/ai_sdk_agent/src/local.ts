import { createMCPClient } from "ai";
import { streamText } from "ai";
import { openai } from "@ai-sdk/openai";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import * as readline from "readline";
import * as path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const dbtMcpDir = path.resolve(__dirname, "../../..");
const envFilePath = process.env.DBT_ENV_FILE ?? path.join(dbtMcpDir, ".env");

async function main() {
  const transport = new StdioClientTransport({
    command: "uvx",
    args: ["--env-file", envFilePath, "dbt-mcp"],
  });

  const mcpClient = await createMCPClient({ transport });
  const tools = await mcpClient.tools();

  console.log("Connected to local dbt MCP server");
  console.log(`Available tools: ${Object.keys(tools).length}`);
  console.log("Type 'quit' to exit\n");

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  const prompt = (query: string): Promise<string> =>
    new Promise((resolve) => rl.question(query, resolve));

  try {
    while (true) {
      const userInput = await prompt("You: ");

      if (userInput.toLowerCase() === "quit") {
        break;
      }

      if (!userInput.trim()) {
        continue;
      }

      process.stdout.write("Assistant: ");

      const result = streamText({
        model: openai("gpt-4o"),
        tools,
        maxSteps: 10,
        system: "You are a helpful assistant with access to dbt tools. Use them to answer questions about the user's dbt project, metrics, models, and data.",
        prompt: userInput,
        onStepFinish: ({ toolCalls }) => {
          if (toolCalls?.length) {
            for (const call of toolCalls) {
              console.log(`\n[Tool: ${call.toolName}]`);
            }
            process.stdout.write("Assistant: ");
          }
        },
      });

      for await (const chunk of (await result).textStream) {
        process.stdout.write(chunk);
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
