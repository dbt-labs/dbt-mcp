import asyncio
from unittest.mock import patch

from dbt_mcp.mcp.server import create_dbt_mcp
from tests.mocks.config import mock_config


def test_initialization():
    with patch("dbt_mcp.config.config.load_config", return_value=mock_config):
        result = asyncio.run(create_dbt_mcp(mock_config))

    assert result is not None
    assert hasattr(result, "usage_tracker")

    tools = asyncio.run(result.list_tools())
    assert isinstance(tools, list)
