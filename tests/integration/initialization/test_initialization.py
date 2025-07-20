import pytest
import asyncio
from unittest.mock import patch

from tests.mocks.config import mock_config


@pytest.mark.filterwarnings("ignore::DeprecationWarning:dbtsl.*")
def test_initialization():
    from dbt_mcp.mcp.server import create_dbt_mcp

    with patch("dbt_mcp.config.config.load_config", return_value=mock_config):
        result = asyncio.run(create_dbt_mcp())

    assert result is not None
    assert hasattr(result, "usage_tracker")
    assert asyncio.run(result.list_tools())
