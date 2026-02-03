import asyncio
import os

from dbt_mcp.config.config import load_config
from dbt_mcp.config.transport import validate_transport
from dbt_mcp.mcp.server import create_dbt_mcp


def main() -> None:
    config = load_config()
    server = asyncio.run(create_dbt_mcp(config))
    transport = validate_transport(os.environ.get("MCP_TRANSPORT", "stdio"))
    
    # Log FASTMCP_HOST for debugging container/K8s deployments
    if config.settings.fastmcp_host:
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"FastMCP server will bind to host: {config.settings.fastmcp_host}")
    
    run_kwargs = {"transport": transport}
    
    # Pass host and port for streamable-http and sse transports
    if transport in ("streamable-http", "sse"):
        run_kwargs["host"] = config.settings.fastmcp_host
        if config.settings.fastmcp_port:
            run_kwargs["port"] = config.settings.fastmcp_port
    
    server.run(**run_kwargs)


if __name__ == "__main__":
    main()
