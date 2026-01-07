import asyncio
import json
from time import time

from mcp.shared.memory import (
    create_connected_server_and_client_session as client_session,
)
from openai import OpenAI
from openai.types.responses.response_input_param import FunctionCallOutput
from openai.types.responses.response_output_message import ResponseOutputMessage

from client.tools import map_tools
from dbt_mcp.config.config import load_config
from dbt_mcp.mcp.server import create_dbt_mcp

LLM_MODEL = "gpt-4o-mini"
TOOL_RESPONSE_TRUNCATION = 100  # set to None for no truncation

llm_client = OpenAI()
messages = []


async def main():
    config = load_config()
    server = await create_dbt_mcp(config)
    async with client_session(server) as client:
        user_role = "user"
        available_tools = (await client.list_tools()).tools
        tools_str = "\n".join(
            [
                f"- {t.name}({', '.join(t.inputSchema['properties'].keys())})"
                for t in available_tools
            ]
        )
        print(f"Available tools:\n{tools_str}")
        while True:
            user_input = input(f"{user_role} > ")
            messages.append({"role": user_role, "content": user_input})
            response_output = None
            tool_call_error = None
            while (
                response_output is None
                or response_output.type == "function_call"
                or tool_call_error is not None
            ):
                tool_call_error = None
                response = llm_client.responses.create(
                    model=LLM_MODEL,
                    input=messages,
                    tools=map_tools(available_tools),
                    parallel_tool_calls=False,
                )
                response_output = response.output[0]
                if isinstance(response_output, ResponseOutputMessage):
                    print(f"{response_output.role} > {response_output.content[0].text}")
                messages.append(response_output)
                if response_output.type != "function_call":
                    continue
                print(
                    f"Calling tool: {response_output.name} with arguments: {response_output.arguments}"
                )
                start_time = time()
                try:
                    tool_response = await client.call_tool(
                        response_output.name,
                        json.loads(response_output.arguments),
                    )
                except Exception as e:
                    tool_call_error = e
                    print(f"Error calling tool: {e}")
                    messages.append(
                        FunctionCallOutput(
                            type="function_call_output",
                            call_id=response_output.call_id,
                            output=str(e),
                        )
                    )
                    continue
                tool_response_str = str(tool_response)
                print(
                    f"Tool responded in {time() - start_time} seconds: "
                    + (
                        f"{tool_response_str[:TOOL_RESPONSE_TRUNCATION]} [TRUNCATED]..."
                        if TOOL_RESPONSE_TRUNCATION
                        and len(tool_response_str) > TOOL_RESPONSE_TRUNCATION
                        else tool_response_str
                    )
                )
                messages.append(
                    FunctionCallOutput(
                        type="function_call_output",
                        call_id=response_output.call_id,
                        output=str(tool_response),
                    )
                )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting.")
