# OpenAI Agent

Examples of using dbt-mcp with OpenAI's agent framework

## Usage

### Local MCP Server

- set up the env var file like described in the README and make sure that the `MCPServerStdio` points to it
- set up the env var `OPENAI_API_KEY` with your OpenAI API key
- run `uv run main.py`

### MCP Streamable HTTP Server

- set up the env var `OPENAI_API_KEY` with your OpenAI API key
- set up the env var `DBT_TOKEN` with your dbt API token
- set up the env var `DBT_PROD_ENV_ID` with your dbt production environment ID
- set up the env var `DBT_HOST` with your dbt host (default is `cloud.getdbt.com`)
- run `uv run main_streamable.py`