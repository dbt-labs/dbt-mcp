# CrewAI Agent Integration 

This guide provides examples of how to use dbt-mpc with CrewAI agent framework.

## Local MCP Server 


- Follow the instructions in the project README to configure the required environment variables for dbt-mcp 

- Ensure that `MCPServerStdio` is correctly configured to point the local MCP SERVER entrypoint. 

- Define the LLM provider API key as an environment variable (e.g. OPENAI_API_KEY).

- For non-local or production-like setups, CrewAI supports alternative MCP server transports:  
    - `MCPServerSSE` - for real-time streaming 
    - `MCPServerHTTP` - for remote or service-based MCP servers

- Run `uv run main.py`