import asyncio
import logging
import os

from dbt_mcp.config.config import load_config
from dbt_mcp.config.transport import validate_transport
from dbt_mcp.mcp.server import create_dbt_mcp

logger = logging.getLogger(__name__)


def main() -> None:
    config = load_config()
    server = asyncio.run(create_dbt_mcp(config))
    transport = validate_transport(os.environ.get("MCP_TRANSPORT", "stdio"))

    logger.info("FastMCP host configured: %s", config.settings.fastmcp_host)
    if config.settings.fastmcp_port is not None:
        logger.info("FastMCP port configured: %s", config.settings.fastmcp_port)

    server.run(transport=transport)


if __name__ == "__main__":
    main()
