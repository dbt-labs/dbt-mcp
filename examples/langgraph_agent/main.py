# mypy: ignore-errors

import asyncio
import os

from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.prebuilt import create_react_agent


def print_stream_item(item):
    if "agent" in item:
        content = [
            part
            for message in item["agent"]["messages"]
            for part in (
                message.content
                if isinstance(message.content, list)
                else [message.content]
            )
        ]
        for c in content:
            if isinstance(c, str):
                print(f"Agent > {c}")
            elif "text" in c:
                print(f"Agent > {c['text']}")
            elif c["type"] == "tool_use":
                print(f"    using tool: {c['name']}")


async def main():
    url = f"https://{os.environ.get('DBT_HOST')}/api/ai/v1/mcp/"
    headers = {
        "x-dbt-user-id": os.environ.get("DBT_USER_ID"),
        "x-dbt-prod-environment-id": os.environ.get("DBT_PROD_ENV_ID"),
        "x-dbt-dev-environment-id": os.environ.get("DBT_DEV_ENV_ID"),
        "Authorization": f"token {os.environ.get('DBT_TOKEN')}",
    }
    client = MultiServerMCPClient(
        {
            "dbt": {
                "url": url,
                "headers": headers,
                "transport": "streamable_http",
            }
        }
    )
    tools = await client.get_tools()
    agent = create_react_agent(
        model="anthropic:claude-3-7-sonnet-latest",
        tools=tools,
        # This allows the agent to have conversational memory.
        checkpointer=InMemorySaver(),
    )
    # This config maintains the conversation thread.
    config = {"configurable": {"thread_id": "1"}}
    while True:
        user_input = input("User > ")
        async for item in agent.astream(
            {"messages": {"role": "user", "content": user_input}},
            config,
        ):
            print_stream_item(item)


if __name__ == "__main__":
    asyncio.run(main())
