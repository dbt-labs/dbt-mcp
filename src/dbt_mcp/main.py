import asyncio
import os

from dbt_mcp.config.config import load_config
from dbt_mcp.config.transport import validate_transport
from dbt_mcp.mcp.server import create_dbt_mcp


def main() -> None:
    config = load_config()
    server = asyncio.run(create_dbt_mcp(config))
    transport = validate_transport(os.environ.get("MCP_TRANSPORT", "stdio"))
    server.run(transport=transport)


if __name__ == "__main__":
    main()
