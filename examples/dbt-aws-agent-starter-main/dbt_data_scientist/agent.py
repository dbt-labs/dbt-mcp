"""Main application entry point for dbt AWS agentcore multi-agent."""

from dotenv import load_dotenv
from bedrock_agentcore import BedrockAgentCoreApp
from strands import Agent

import prompts

from tools import (
    dbt_compile,
    dbt_mcp_tool,
    dbt_model_analyzer_agent
)

# Load environment variables
load_dotenv()

# Initialize the Bedrock Agent Core App
app = BedrockAgentCoreApp()

# Initialize the main agent
dbt_agent = Agent(
    system_prompt=prompts.ROOT_AGENT_INSTR,
    callback_handler=None,
    tools=[dbt_compile, dbt_mcp_tool, dbt_model_analyzer_agent]
)

@app.entrypoint
def invoke(payload):
    """Main AI agent function with access to dbt tools."""
    user_message = payload.get("prompt", "Hello! How can I help you today?")
    
    try:
        # Process the user message with the dbt agent
        result = dbt_agent(user_message)
        
        # Extract the response content
        response_content = str(result)
        
        return {"result": response_content}
    
    except Exception as e:
        return {"result": f"Error processing your request: {str(e)}"}

# Example usage for local testing
if __name__ == "__main__":
    print("\ndbt's Assistant Strands Agent\n")
    print("Ask a question about our dbt mcp server, our local fusion compiler, or our data model analyzer and I'll route it to the appropriate specialist.")
    print("Type 'exit' to quit.")

    # Interactive loop for local testing
    while True:
        try:
            user_input = input("\n> ")
            if user_input.lower() == "exit":
                print("\nGoodbye! 👋")
                break

            response = dbt_agent(user_input)
            
            # Extract and print only the relevant content from the specialized agent's response
            content = str(response)
            print(content)
            
        except KeyboardInterrupt:
            print("\n\nExecution interrupted. Exiting...")
            break
        except Exception as e:
            print(f"\nAn error occurred: {str(e)}")
            print("Please try asking a different question.")
