import asyncio

from dbt_mcp.config.config import load_config
from dbt_mcp.mcp.server import create_dbt_mcp


def main() -> None:
    config = load_config()
    asyncio.run(create_dbt_mcp(config)).run()


main()
