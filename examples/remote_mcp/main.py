import asyncio
import json

from mcp.types import TextContent

from remote_mcp.session import session_context


async def main():
    async with session_context() as session:
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
