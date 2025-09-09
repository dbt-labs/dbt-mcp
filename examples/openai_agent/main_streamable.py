# mypy: ignore-errors

import asyncio
import os

from agents import Agent, Runner, trace
from agents.mcp import create_static_tool_filter
from agents.mcp.server import MCPServerStreamableHttp
from agents.stream_events import RawResponsesStreamEvent, RunItemStreamEvent
from openai.types.responses import ResponseCompletedEvent, ResponseOutputMessage


def print_tool_call(tool_name, params, color="yellow", show_params=True):
    # Define color codes for different colors
    # we could use a library like colorama but this avoids adding a dependency
    color_codes = {
        "grey": "\033[37m",
        "yellow": "\033[93m",
    }
    color_code_reset = "\033[0m"

    color_code = color_codes.get(color, color_codes["yellow"])
    msg = f"Calling the tool {tool_name}"
    if show_params:
        msg += f" with params {params}"
    print(f"{color_code}# {msg}{color_code_reset}")


def handle_event_printing(event, show_tools_calls=True):
    if type(event) is RunItemStreamEvent and show_tools_calls:
        if event.name == "tool_called":
            print_tool_call(
                event.item.raw_item.name,
                event.item.raw_item.arguments,
                color="grey",
                show_params=True,
            )

    if type(event) is RawResponsesStreamEvent:
        if type(event.data) is ResponseCompletedEvent:
            for output in event.data.response.output:
                if type(output) is ResponseOutputMessage:
                    print(output.content[0].text)


async def main(inspect_events_tools_calls=False):
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
                "get_metrics_compiled_sql",
            ],
        ),
    ) as server:
        agent = Agent(
            name="Assistant",
            instructions="Use the tools to answer the user's questions. Do not invent data or sample data.",
            mcp_servers=[server],
            model="gpt-5",
        )
        with trace(workflow_name="Conversation"):
            conversation = []
            result = None
            while True:
                if result:
                    conversation = result.to_input_list()
                conversation.append({"role": "user", "content": input("User > ")})

                if inspect_events_tools_calls:
                    async for event in Runner.run_streamed(
                        agent, conversation
                    ).stream_events():
                        handle_event_printing(event, show_tools_calls=True)
                else:
                    result = await Runner.run(agent, conversation)
                    print(result.final_output)


if __name__ == "__main__":
    try:
        asyncio.run(main(inspect_events_tools_calls=True))
    except KeyboardInterrupt:
        print("\nExiting.")
