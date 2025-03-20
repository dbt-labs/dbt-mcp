import json
import os
import time
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from dbtsl import SemanticLayerClient
import requests
from semantic_layer.types import DimensionToolResponse, MetricToolResponse

# Load environment variables from .env file
load_dotenv()

# Create the MCP server
mcp = FastMCP("dbt")

# Global config to store connection information
CONFIG = {
    "host": os.environ.get("DBT_HOST"),
    "environment_id": os.environ.get("DBT_ENV_ID"),
    "token": os.environ.get("DBT_TOKEN"),
    "is_connected": False
}

host = os.environ.get("DBT_HOST")
environment_id = os.environ.get("DBT_ENV_ID")
token = os.environ.get("DBT_TOKEN")

if not host or not environment_id or not token:
    raise ValueError("Missing required environment variables: DBT_HOST, DBT_ENV_ID, DBT_TOKEN.")

semantic_layer_client = SemanticLayerClient(
    environment_id=int(environment_id),
    auth_token=token,
    host=host,
)

def check_required_env_vars() -> list[str]:
    """Check if all required environment variables are set"""
    missing_vars = []
    for var in ["DBT_HOST", "DBT_ENV_ID", "DBT_TOKEN"]:
        if not os.environ.get(var):
            missing_vars.append(var)
    return missing_vars

def test_sl_connection() -> bool:
    """Test the connection to the semantic layer"""
    missing_vars = check_required_env_vars()
    if missing_vars:
        print(f"Cannot auto-connect: Missing environment variables: {', '.join(missing_vars)}")
        print("Please set the following environment variables:")
        print("  - DBT_HOST: The dbt Semantic Layer host")
        print("  - DBT_ENV_ID: The dbt environment ID")
        print("  - DBT_TOKEN: The service token")
        return False

    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        query = f"""{{
          environmentInfo(environmentId: "{CONFIG['environment_id']}") {{
            dialect
          }}
        }}"""

        print(f"Auto-connecting to semantic layer at {CONFIG['host']}...")

        response = requests.post(
            url,
            headers=headers,
            json={"query": query}
        )

        data = response.json()
        if "errors" not in data:
            CONFIG["is_connected"] = True
            print(f"Successfully connected to {CONFIG['host']}")
            return True
        else:
            print(f"Connection error: {data['errors']}")
            return False
    except Exception as e:
        print(f"Failed to connect: {str(e)}")
        return False

test_sl_connection()

@mcp.tool()
def list_metrics() -> list[MetricToolResponse]:
    """
    List all metrics from the dbt Semantic Layer
    """
    with semantic_layer_client.session():
        return [
            MetricToolResponse(
                name=m.name,
                type=m.type,
                label=m.label,
                description=m.description,
            )
            for m in semantic_layer_client.metrics()
        ]

@mcp.tool()
def get_dimensions(metrics: list[str]) -> list[DimensionToolResponse]:
    """
    Get available dimensions for specified metrics

    Args:
        metrics: List of metric names or a single metric name
    """
    with semantic_layer_client.session():
        return [
            DimensionToolResponse(
                name=d.name,
                type=d.type,
                description=d.description,
                label=d.label,
                granularities=d.queryable_time_granularities
            )
            for d in semantic_layer_client.dimensions(metrics=metrics)
        ]

@mcp.tool()
def query_metrics(
    metrics: list[str],
    group_by: list[str] | None = None,
    time_grain: str | None = None,
    limit: int | None = None
):
    """
    Query metrics with optional grouping and filtering

    Args:
        metrics: List of metric names
        group_by: Optional list of dimensions to group by
        time_grain: Optional time grain (DAY, WEEK, MONTH, QUARTER, YEAR)
        limit: Optional limit for number of results
    """
    try:
        # Generate metric list string for GraphQL
        metric_list = ", ".join([f"{{name: \"{metric}\"}}" for metric in metrics])

        # Build group_by section if needed
        group_by_section = ""
        if group_by:
            groups = []
            for dim in group_by:
                if dim == "metric_time" and time_grain:
                    groups.append(f'{{name: "{dim}", grain: {time_grain}}}')
                else:
                    groups.append(f'{{name: "{dim}"}}')
            group_by_section = f"groupBy: [{', '.join(groups)}]"

        # Build limit section if needed
        limit_section = f"limit: {limit}" if limit else ""

        # Build create query mutation with direct string construction
        mutation = f"""
        mutation {{
          createQuery(
            environmentId: "{CONFIG['environment_id']}"
            metrics: [{metric_list}]
            {group_by_section}
            {limit_section}
          ) {{
            queryId
          }}
        }}
        """

        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}

        print(f"Executing GraphQL mutation: {mutation}")

        # Execute create query mutation
        response = requests.post(
            url,
            headers=headers,
            json={"query": mutation}
        )
        response.raise_for_status()
        create_data = response.json()

        if "errors" in create_data:
            return f"GraphQL error: {create_data['errors']}"

        # Get query ID
        query_id = create_data["data"]["createQuery"]["queryId"]

        # Poll for results
        max_attempts = 30
        attempts = 0
        query_result = None

        while attempts < max_attempts:
            attempts += 1

            # Query for results
            result_query = f"""
            {{
              query(environmentId: "{CONFIG['environment_id']}", queryId: "{query_id}") {{
                status
                error
                jsonResult(encoded: false)
              }}
            }}
            """

            print(f"Polling with query: {result_query}")

            response = requests.post(
                url,
                headers=headers,
                json={"query": result_query}
            )
            response.raise_for_status()
            result_data = response.json()

            if "errors" in result_data:
                return f"GraphQL error: {result_data['errors']}"

            query_result = result_data["data"]["query"]

            # Check status
            if query_result["status"] == "FAILED":
                return f"Query failed: {query_result['error']}"
            elif query_result["status"] == "SUCCESSFUL":
                break

            # Wait before polling again
            time.sleep(1)

        if attempts >= max_attempts:
            return "Query timed out. Please try again or simplify your query."

        # Parse and return results
        if query_result["jsonResult"]:
            # Return the raw JSON result
            return query_result["jsonResult"]
        else:
            return "No results returned."
    except Exception as e:
        return f"Error querying metrics: {str(e)}"

if __name__ == "__main__":
    # Check environment variables and connection status
    missing_vars = check_required_env_vars()
    if missing_vars:
        print("\n⚠️ Missing required environment variables:")
        print(f"   {', '.join(missing_vars)}")
        print("\nPlease set these environment variables before starting the server.")
        print("You can set them using:")
        for var in missing_vars:
            print(f"export {var}=<your-value>")
        print()

    # Try to connect if auto-connect is enabled
    if not CONFIG["is_connected"]:
        test_sl_connection()

    # Show connection status
    if CONFIG["is_connected"]:
        print("\n✅ Ready to use! The semantic layer connection is active.")
        print("Available metrics: Use 'list_metrics()' to see all metrics\n")
    else:
        print("\n⚠️ Not connected to the semantic layer.")
        print("Make sure environment variables are set and use 'connect()' to establish a connection.\n")

    mcp.run()