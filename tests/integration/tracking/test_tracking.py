import pytest
from dbtlabs_vortex.producer import shutdown
from dotenv import load_dotenv

from dbt_mcp.config.config import load_config
from dbt_mcp.mcp.server import create_dbt_mcp


@pytest.mark.asyncio
async def test_tracking():
    load_dotenv()
    config = load_config()
    await (await create_dbt_mcp(config)).call_tool("list_metrics", {"foo": "bar"})
    shutdown()
