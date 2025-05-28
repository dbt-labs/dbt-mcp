import pytest
from dbtlabs_vortex.producer import shutdown

from dbt_mcp.mcp.server import dbt_mcp


@pytest.mark.asyncio
async def test_tracking():
    await dbt_mcp.call_tool("list_metrics", {"foo": "bar"})
    shutdown()
