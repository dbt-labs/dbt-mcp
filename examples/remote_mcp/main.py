import asyncio
import contextlib
import json
import os
from collections.abc import AsyncGenerator

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.types import TextContent


@contextlib.asynccontextmanager
async def session_context(
    *, url: str, headers: dict[str, str]
) -> AsyncGenerator[ClientSession, None]:
    async with (
        streamablehttp_client(
            url=url,
            headers=headers,
        ) as (
            read_stream,
            write_stream,
            _,
        ),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        yield session


async def main():
    async with session_context(
        url=f"https://{os.environ.get('DBT_HOST')}/api/ai/v1/mcp/",
        headers={
            "Authorization": f"token {os.environ.get('DBT_TOKEN')}",
            "x-dbt-prod-environment-id": os.environ.get("DBT_PROD_ENV_ID"),
        },
    ) as session:
        available_metrics = await session.call_tool(
            name="list_metrics",
            arguments={},
        )
        metrics_content = [
            t for t in available_metrics.content if isinstance(t, TextContent)
        ]
        metrics_names = [json.loads(m.text)["name"] for m in metrics_content]
        print(f"Available metrics: {', '.join(metrics_names)}\n")
        num_food_orders = await session.call_tool(
            name="query_metrics",
            arguments={
                "metrics": [
                    "food_orders",
                ],
            },
        )
        num_food_order_content = num_food_orders.content[0]
        assert isinstance(num_food_order_content, TextContent)
        print(f"Number of food orders: {num_food_order_content.text}")


if __name__ == "__main__":
    asyncio.run(main())
