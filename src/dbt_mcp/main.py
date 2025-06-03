import argparse
import asyncio

from dbt_mcp.mcp.server import create_dbt_mcp


def main() -> None:
    parser = argparse.ArgumentParser(description="dbt MCP Server")
    parser.add_argument(
        "--env-file",
        type=str,
        help="Path to the .env file to load environment variables from",
    )
    args = parser.parse_args()
    
    asyncio.run(create_dbt_mcp(env_file=args.env_file)).run()


if __name__ == "__main__":
    main()
