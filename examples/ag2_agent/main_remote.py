"""AG2 single-agent example with dbt-mcp via Streamable HTTP transport.

Connects to the hosted dbt Cloud MCP endpoint. Mirrors the pattern used
in the LangGraph and PydanticAI examples.

Required environment variables:
  OPENAI_API_KEY
  DBT_TOKEN         - dbt Cloud API token
  DBT_PROD_ENV_ID   - dbt production environment ID
  DBT_HOST          - dbt Cloud host (optional, default: cloud.getdbt.com)
"""

import asyncio
import os

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from autogen import ConversableAgent, LLMConfig
from autogen.mcp import create_toolkit


async def main() -> None:
    host = os.environ.get("DBT_HOST", "cloud.getdbt.com")
    token = os.environ["DBT_TOKEN"]
    prod_env_id = os.environ["DBT_PROD_ENV_ID"]

    mcp_url = f"https://{host}/api/ai/v1/mcp/"
    headers = {
        "Authorization": f"token {token}",
        "x-dbt-prod-environment-id": prod_env_id,
    }

    llm_config = LLMConfig(
        api_type="openai",
        model="gpt-4.1",
        api_key=os.environ["OPENAI_API_KEY"],
    )

    async with (
        streamablehttp_client(mcp_url, headers=headers) as (read, write, _),
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

        print("dbt MCP Agent ready (AG2 + dbt Cloud). Type 'quit' to exit.\n")
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
