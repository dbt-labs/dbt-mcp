"""AG2 single-agent example with dbt-mcp via stdio transport.

Connects to a locally running dbt-mcp server launched as a subprocess.
Mirrors the pattern used in the CrewAI and Google ADK examples.

Run from the repo root so the .env file is discovered:
  uv run examples/ag2_agent/main_stdio.py

The .env file should be configured as described in the root README.
"""

import asyncio
import os
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from autogen import ConversableAgent, LLMConfig
from autogen.mcp import create_toolkit


async def main() -> None:
    dbt_mcp_dir = Path(__file__).parent.parent.parent

    server_params = StdioServerParameters(
        command="uvx",
        args=["--env-file", str(dbt_mcp_dir / ".env"), "dbt-mcp"],
    )

    llm_config = LLMConfig(
        api_type="openai",
        model="gpt-4.1",
        api_key=os.environ["OPENAI_API_KEY"],
    )

    async with (
        stdio_client(server_params) as (read, write),
        ClientSession(read, write) as session,
    ):
        await session.initialize()
        toolkit = await create_toolkit(session=session)

        dbt_agent = ConversableAgent(
            name="dbt_assistant",
            system_message=(
                "You are a senior analytics engineer with deep expertise in dbt. "
                "Use your tools to inspect the project, answer questions about "
                "models, metrics, lineage, and data quality. "
                "Never guess — always use a tool."
            ),
            llm_config=llm_config,
            human_input_mode="NEVER",
        )

        toolkit.register_for_llm(dbt_agent)
        toolkit.register_for_execution(dbt_agent)

        print("dbt MCP Agent ready (AG2 + local). Type 'quit' to exit.\n")
        while True:
            user_input = input("User > ").strip()
            if user_input.lower() in {"quit", "exit", "q"}:
                break
            if not user_input:
                continue
            result = await dbt_agent.a_run(
                message=user_input,
                tools=toolkit.tools,
                max_turns=10,
            )
            await result.process()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting.")
    except Exception as e:
        print(f"Error: {e}")
