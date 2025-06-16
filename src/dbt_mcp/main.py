import asyncio

from dbt_mcp.mcp.server import create_dbt_mcp


def main() -> None:
    mcp = asyncio.run(create_dbt_mcp())
    mcp.run(transport="sse")

main()
