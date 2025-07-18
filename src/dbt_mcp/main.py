import asyncio

from dotenv import load_dotenv

from dbt_mcp.config.config import load_config
from dbt_mcp.mcp.server import create_dbt_mcp


def main() -> None:
    load_dotenv()
    config = load_config()
    asyncio.run(create_dbt_mcp(config)).run()


main()
