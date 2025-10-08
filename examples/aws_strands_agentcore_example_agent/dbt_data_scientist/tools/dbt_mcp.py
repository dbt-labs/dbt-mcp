"""dbt MCP Tool - Remote dbt MCP server connection for AWS Bedrock Agent Core."""
import os
from strands import Agent, tool
from mcp import ClientSession
from dotenv import load_dotenv
from mcp.client.streamable_http import streamablehttp_client
from strands.tools.mcp.mcp_client import MCPClient

# Load environment variables
load_dotenv()
DBT_MCP_URL = os.environ.get("DBT_MCP_URL")
DBT_USER_ID = os.environ.get("DBT_USER_ID")
DBT_PROD_ENV_ID = os.environ.get("DBT_PROD_ENV_ID")
DBT_DEV_ENV_ID = os.environ.get("DBT_DEV_ENV_ID")
DBT_ACCOUNT_ID = os.environ.get("DBT_ACCOUNT_ID")
DBT_TOKEN = os.environ.get("DBT_TOKEN")

DBT_MCP_AGENT_SYSTEM_PROMPT = """
You are a dbt MCP server expert, a specialized assistant for dbt MCP server analysis and troubleshooting. Your capabilities include:

When asked to 'view features available on the dbt MCP server', or 'ask about a specific tool or function', inspect the dbt MCP server and return a result based on the available tools and functions.
"""

# Create MCP client once at module level
def create_dbt_mcp_client():
    """Create the dbt MCP client with proper configuration."""
    load_dotenv()
    
    if not DBT_MCP_URL:
        raise ValueError("DBT_MCP_URL environment variable is required")
    
    return MCPClient(lambda: streamablehttp_client(
        url=DBT_MCP_URL,
        headers={
            "x-dbt-user-id": DBT_USER_ID,
            "x-dbt-prod-environment-id": DBT_PROD_ENV_ID,
            "x-dbt-dev-environment-id": DBT_DEV_ENV_ID,
            "x-dbt-account-id": DBT_ACCOUNT_ID,
            "Authorization": f"token {DBT_TOKEN}",
        },
    ))

# Global MCP client instance
dbt_mcp_client = create_dbt_mcp_client()

@tool
def dbt_mcp_tool(query: str) -> str:
    """
    Connects to remote dbt MCP server and executes queries.
    
    Args:
        query: The user's question about dbt MCP server functionality
        
    Returns:
        String response with dbt MCP server results
    """
    try:
        print(f"Connecting to dbt MCP server for query: {query}")
        
        with dbt_mcp_client:
            # Get available tools from MCP server
            tools = dbt_mcp_client.list_tools_sync()
            
            if not tools:
                return "No tools available on the dbt MCP server."
            
            # If user asks to list tools, return them
            if "list" in query.lower() and ("tool" in query.lower() or "feature" in query.lower()):
                tool_list = "\n".join([f"- {tool.name}: {tool.description}" for tool in tools])
                return f"Available dbt MCP tools:\n{tool_list}"
            
            # For other queries, try to find and execute the most relevant tool
            # This is a simplified approach - in practice you'd want more sophisticated routing
            if tools:
                # Try to call the first available tool as an example
                first_tool = tools[0]
                try:
                    result = dbt_mcp_client.call_tool_sync(first_tool.name, {})
                    return f"Executed {first_tool.name}: {result}"
                except Exception as e:
                    return f"Error executing {first_tool.name}: {str(e)}"
            
            return f"Found {len(tools)} tools on dbt MCP server. Use 'list tools' to see them."
    
    except Exception as e:
        return f"Error connecting to dbt MCP server: {str(e)}"

def test_connection():
    """Test function to verify MCP connectivity."""
    print("🧪 Testing dbt MCP connection...")
    
    try:
        with dbt_mcp_client:
            tools = dbt_mcp_client.list_tools_sync()
            print(f"✅ Successfully connected to dbt MCP server!")
            print(f"📋 Found {len(tools)} available tools:")
            
            for i, tool in enumerate(tools, 1):
                print(f"  {i}. {tool.name}: {tool.description}")
            
            return True
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

# Test the connection when this module is run directly
if __name__ == "__main__":
    print("🔌 dbt MCP Server Connection Test")
    print("=" * 40)
    
    # Check environment variables
    load_dotenv()
    required_vars = ["DBT_MCP_URL", "DBT_TOKEN", "DBT_USER_ID", "DBT_PROD_ENV_ID"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("Please set these in your .env file or environment.")
        sys.exit(1)
    
    # Test connection
    success = test_connection()
    
    if success:
        print("\n🎉 MCP connection test passed!")
        print("You can now run the agent: python dbt_data_scientist/agent.py")
    else:
        print("\n💥 MCP connection test failed!")
        print("Please check your configuration and try again.")
    
    sys.exit(0 if success else 1)