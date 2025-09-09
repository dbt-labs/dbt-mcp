# Pydantic AI Agent

An example of using Pydantic AI with the remote dbt MCP server.

## Config

Set the following environment variables:
- `OPENAI_API_KEY` (or the API key for any other model supported by PydanticAI)
- `DBT_TOKEN`
- `DBT_PROD_ENV_ID`
- `DBT_HOST` (if not using the default `cloud.getdbt.com`)

## Usage

`uv run main.py`