import pytest
from dbtlabs_vortex.producer import shutdown

from dbt_mcp.mcp.server import create_dbt_mcp


@pytest.mark.asyncio
async def test_tracking():
    await (await create_dbt_mcp()).call_tool("list_metrics", {"foo": "bar"})
    shutdown()
