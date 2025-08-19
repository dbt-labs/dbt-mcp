# mypy: ignore-errors

import asyncio
import os

from agents import Agent, Runner, trace
from agents.mcp import create_static_tool_filter
from agents.mcp.server import MCPServerStreamableHttp


async def main():
    prod_environment_id = os.environ.get("DBT_PROD_ENV_ID", os.getenv("DBT_ENV_ID"))
    token = os.environ.get("DBT_TOKEN")
    host = os.environ.get("DBT_HOST", "cloud.getdbt.com")

    async with MCPServerStreamableHttp(
        name="dbt",
        params={
            "url": f"https://{host}/api/ai/v1/mcp/",
            "headers": {
                "Authorization": f"token {token}",
                "x-dbt-prod-environment-id": prod_environment_id,
            },
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
