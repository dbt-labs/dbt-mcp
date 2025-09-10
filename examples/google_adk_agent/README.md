# Google ADK Agent for dbt MCP

An example of using Google Agent Development Kit  with the remote dbt MCP server.

## Config

Set the following environment variables:
- `GOOGLE_GENAI_API_KEY` (or the API key for any other model supported by google ADK)
- `ADK_MODEL` (Choose a different model (default: gemini-2.0-flash))
- `DBT_TOKEN`
- `DBT_PROD_ENV_ID`
- `DBT_HOST` (if not using the default `cloud.getdbt.com`)
- `DBT_PROJECT_DIR` (if using dbt core)

### Usage

`uv run main.py`