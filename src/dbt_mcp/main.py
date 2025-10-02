import asyncio

from dbt_mcp.config.config import load_config
from dbt_mcp.mcp.server import create_dbt_mcp
from dbt_mcp.telemetry.logging import configure_file_logging


def main() -> None:
    configure_file_logging()
    config = load_config()
    asyncio.run(create_dbt_mcp(config)).run()


main()
