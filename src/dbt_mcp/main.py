import asyncio

from dbt_mcp.mcp.server import create_dbt_mcp


def main() -> None:
    asyncio.run(create_dbt_mcp()).run()


main()
