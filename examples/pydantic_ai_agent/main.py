import asyncio
import os
from pydantic_ai import Agent, RunContext  # type: ignore
from pydantic_ai.mcp import MCPServerStreamableHTTP  # type: ignore
from pydantic_ai.messages import (  # type: ignore
    FunctionToolCallEvent,
)


async def main():
    """Start a conversation using PydanticAI with an HTTP MCP server."""

    prod_environment_id = os.environ.get("DBT_PROD_ENV_ID", os.getenv("DBT_ENV_ID"))
    token = os.environ.get("DBT_TOKEN")
    host = os.environ.get("DBT_HOST", "cloud.getdbt.com")

    # Configure MCP server connection
    mcp_server_url = f"https://{host}/api/ai/v1/mcp/"
    mcp_server_headers = {
        "Authorization": f"token {token}",
        "x-dbt-prod-environment-id": prod_environment_id,
    }
    server = MCPServerStreamableHTTP(url=mcp_server_url, headers=mcp_server_headers)

    # Initialize the agent with OpenAI model and MCP tools
    # PydanticAI also supports Anthropic models, Google models, and more
    agent = Agent(
        "openai:gpt-5",
        toolsets=[server],
        system_prompt="You are a helpful AI assistant with access to MCP tools.",
    )

    print("Starting conversation with PydanticAI + MCP server...")
    print("Type 'quit' to exit\n")

    async with agent:
        while True:
            try:
                user_input = input("You: ").strip()

                if user_input.lower() in ["quit", "exit", "q"]:
                    print("Goodbye!")
                    break

                if not user_input:
                    continue

                # Event handler for real-time tool call detection
                async def event_handler(ctx: RunContext, event_stream):
                    async for event in event_stream:
                        if isinstance(event, FunctionToolCallEvent):
                            print(f"\n🔧 Tool called: {event.part.tool_name}")
                            print(f"   Arguments: {event.part.args}")
                            print("Assistant: ", end="", flush=True)

                # Stream the response with real-time events
                print("Assistant: ", end="", flush=True)
                async with agent.run_stream(
                    user_input, event_stream_handler=event_handler
                ) as result:
                    async for text in result.stream_text(delta=True):
                        print(text, end="", flush=True)
                print()  # New line after response

            except KeyboardInterrupt:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
