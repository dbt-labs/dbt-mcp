import asyncio
import os
from pathlib import Path

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import (
    StdioConnectionParams,
    StdioServerParameters,
)
from google.genai import types


async def main():
    if not os.environ.get("GOOGLE_GENAI_API_KEY"):
        print("Missing GOOGLE_GENAI_API_KEY environment variable.")
        print("Get your API key from: https://aistudio.google.com/apikey")
        return

    dbt_mcp_dir = Path(__file__).parent.parent.parent

    toolset = McpToolset(
        connection_params=StdioConnectionParams(
            server_params=StdioServerParameters(
                command="uvx",
                args=["--env-file", f"{dbt_mcp_dir}/.env", "dbt-mcp"],
                env=os.environ.copy(),
            )
        )
    )

    agent = LlmAgent(
        name="dbt_assistant",
        model=os.environ.get("ADK_MODEL", "gemini-2.0-flash"),
        instruction="You are a helpful dbt assistant with access to dbt tools via MCP Tools.",
        tools=[toolset],
    )

    runner = Runner(
        agent=agent,
        app_name="dbt_adk_agent",
        session_service=InMemorySessionService(),
    )

    await runner.session_service.create_session(
        app_name="dbt_adk_agent", user_id="user", session_id="session_1"
    )

    print("Google ADK + dbt MCP Agent ready! Type 'quit' to exit.\n")

    while True:
        try:
            user_input = input("User > ").strip()

            if user_input.lower() in {"quit", "exit", "q"}:
                print("Goodbye!")
                break

            if not user_input:
                continue

            events = runner.run(
                user_id="user",
                session_id="session_1",
                new_message=types.Content(
                    role="user", parts=[types.Part(text=user_input)]
                ),
            )

            for event in events:
                if hasattr(event, "content") and hasattr(event.content, "parts"):
                    for part in event.content.parts:
                        if hasattr(part, "text") and part.text:
                            print(f"Assistant: {part.text}")

        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting.")
