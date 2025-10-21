from strands import Agent, tool
from strands_tools import python_repl, shell, file_read, file_write, editor
import os, json, subprocess

from dotenv import load_dotenv

DBT_COMPILE_ASSISTANT_SYSTEM_PROMPT = """
You are a dbt pipeline expert, a specialized assistant for dbt pipeline analysis and troubleshooting. Your capabilities include:

When asked to 'find a problem' or 'compile a project' on your local dbt project, inspect its JSON logs,
    and then:
    1) Summarize the problem(s) (file, node, message).
    2) Recommend a concrete fix in 1-3 bullet points (e.g., correct ref(), add column, fix Jinja).
    3) If no errors, say compile is clean and suggest next step (e.g., run build state:modified+).
"""

@tool
def dbt_compile(query: str) -> str:
    """
    Runs `dbt compile --log-format json` in the DBT_ROOT and returns:
    returncode
    logs: list of compiled JSON events (dbt emits JSON per line)
    """
    # Load environment variables from .env file
    load_dotenv()

    # Get the DBT_ROOT environment variable, default to current directory
    dbt_project_location = os.getenv("DBT_PROJECT_LOCATION", os.getcwd())
    dbt_executable = os.getenv("DBT_EXECUTABLE")
    
    print(f"Running dbt compile in: {dbt_project_location}")
    print(f"Running dbt executable located here: {dbt_executable}")

    proc = subprocess.run(
            [dbt_executable, "compile", "--log-format", "json"],
            cwd=dbt_project_location,
            text=True,
            capture_output=True
        )
    print(proc)
    logs: List[Dict] = []
    for stream in (proc.stdout, proc.stderr):
        for line in stream.splitlines():
            try:
                logs.append(json.loads(line))
            except json.JSONDecodeError:
                # ignore non-JSON lines quietly
                pass
    output_of_compile = {"returncode": proc.returncode, "logs": logs}

    # Format the query for the compile agent with clear instructions
    formatted_query = f"User will have asked to compile a dbt project. Summarize the results of the compile command output: {output_of_compile}"
    
    try:
        print("Routed to dbt compile agent")
        # Create the dbt compile agent with relevant tools
        dbt_compile_agent = Agent(
            system_prompt=DBT_COMPILE_ASSISTANT_SYSTEM_PROMPT,
            tools=[],
        )
        agent_response = dbt_compile_agent(formatted_query)
        text_response = str(agent_response)

        if len(text_response) > 0:
            return text_response
        
        return "I apologize, but I couldn't process your dbt compile question. Please try rephrasing or providing more specific details about what you're trying to learn or accomplish."
    except Exception as e:
        # Return specific error message for dbt compile processing
        return f"Error processing your dbt compile query: {str(e)}"