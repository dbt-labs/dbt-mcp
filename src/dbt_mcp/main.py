import asyncio
import logging
import os

from dbt_mcp.config.config import load_config
from dbt_mcp.config.transport import validate_transport
from dbt_mcp.mcp.server import create_dbt_mcp

logger = logging.getLogger(__name__)


def main() -> None:
    config = load_config()

    multi_project_enabled = os.environ.get(
        "DBT_MCP_MULTI_PROJECT_ENABLED", ""
    ).lower() in ("true", "1", "yes")

    if multi_project_enabled:
        logger.info(
            "DBT_MCP_MULTI_PROJECT_ENABLED=true -> Multi-project mode (Server B)"
        )
        raise NotImplementedError(
            "Multi-project mode is not yet implemented. "
            "It will be available in a future release."
        )
    else:
        logger.info("Multi-project mode disabled -> Env-var mode (Server A)")
        server = asyncio.run(create_dbt_mcp(config))

    transport = validate_transport(os.environ.get("MCP_TRANSPORT", "stdio"))
    server.run(transport=transport)


if __name__ == "__main__":
    main()
