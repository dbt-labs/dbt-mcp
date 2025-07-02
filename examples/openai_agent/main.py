# mypy: ignore-errors

import asyncio
from pathlib import Path

from agents import Agent, Runner, trace
from agents.mcp import create_static_tool_filter
from agents.mcp.server import MCPServerStdio


async def main():
    dbt_mcp_dir = Path(__file__).parent.parent.parent
    async with MCPServerStdio(
        name="dbt",
        params={
            "command": "uvx",
            "args": [
                "--env-file",
                # This file should contain config described in the root README.md
                f"{dbt_mcp_dir}/.env",
                "dbt-mcp",
            ],
        },
        client_session_timeout_seconds=20,
        cache_tools_list=True,
        tool_filter=create_static_tool_filter(
            allowed_tool_names=[
                "list_metrics",
                "get_dimensions",
                "get_entities",
                "query_metrics",
            ],
        ),
    ) as server:
        agent = Agent(
            name="Assistant",
            instructions="Use the tools to answer the user's questions",
            mcp_servers=[server],
        )
        with trace(workflow_name="Conversation"):
            conversation = []
            result = None
            while True:
                if result:
                    conversation = result.to_input_list()
                conversation.append({"role": "user", "content": input("User > ")})
                result = await Runner.run(agent, conversation)
                print(result.final_output)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting.")
