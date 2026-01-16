import asyncio
from pathlib import Path

from crewai import Agent, LLM, Task, Crew
from crewai.mcp import MCPServerStdio


async def create_mcp_server(dbt_mcp_dir: str) -> MCPServerStdio:
    return MCPServerStdio(
        command="uvx",
        args=["--env-file", f"{dbt_mcp_dir}/.env", "dbt-mcp"],
        tool_filter=None,
        cache_tools_list=True,
    )


async def main():
    dbt_mcp_dir = Path(__file__).parent.parent.parent
    llm = LLM(model="gpt-4.1")
    server = await create_mcp_server(dbt_mcp_dir=dbt_mcp_dir)

    dbt_agent = Agent(
        role="dbt agent",
        goal="Ask the user for a question before taking any action.",
        backstory=(
            "You are a senior analytics engineer with deep expertise in dbt projects. "
            "You understand dbt models, sources, macros, tests, documentation, and "
            "project structure, and you know when to inspect the repository using tools "
            "instead of guessing."
        ),
        mcps=[server],
        llm=llm,
    )

    # Create a task
    dbt_task = Task(
        description="Answer the user question related to dbt using the available tools.",
        expected_output="A clear answer to user's question obtained via the tools.",
        agent=dbt_agent,
        human_input=True,
    )
    analysis_crew = Crew(
        agents=[dbt_agent], tasks=[dbt_task], memory=True, verbose=False
    )

    _results = await analysis_crew.kickoff_async()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Execution cancelled by user.")
    except Exception as e:
        print(f"Error occurred: {e}")