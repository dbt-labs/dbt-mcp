# Google ADK Local Agent for dbt MCP

This example demonstrates how to integrate Google's AI Development Kit (ADK) with a **local dbt project** using dbt-mcp. Unlike cloud-based examples, this agent works with your local dbt development environment, making it accessible for all developers without requiring dbt Cloud credentials.

## Prerequisites

1. **uv** (for dependency management)
2. **A local dbt project** (configured with profiles.yml)
3. **Google Gemini API Key** (get free at [aistudio.google.com](https://aistudio.google.com/apikey))
4. **dbt MCP server setup** (see main README)


### Set Environment Variables in the .env

```bash
# Required: Google Gemini API key
export GOOGLE_GENAI_API_KEY=your-gemini-api-key

# Required: Path to your dbt project
export DBT_PROJECT_DIR=/path/to/your/dbt/project
# Example: export DBT_PROJECT_DIR=/Users/username/my-dbt-project

# Optional: Choose a different model (default: gemini-2.0-flash)
export ADK_MODEL=gemini-2.0-flash
```

### Usage

`uv run main.py`