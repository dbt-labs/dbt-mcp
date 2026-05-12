from unittest.mock import mock_open, patch

import pytest
from pytest import MonkeyPatch

from dbt_mcp.dbt_cli.tools import register_dbt_cli_tools
from tests.mocks.config import mock_dbt_cli_config


@pytest.fixture
def mock_process():
    class MockProcess:
        returncode = 0

        def communicate(self, timeout=None):
            return "command output", ""

    return MockProcess()


def test_manifest_uses_utf8_encoding(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp
):
    """Regression test for #594: open() without encoding defaults to CP-1252 on Windows."""

    def mock_popen(args, **kwargs):
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        mock_dbt_cli_config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )

    with patch("builtins.open", mock_open(read_data='{"nodes": {}}')) as mock_file:
        tools["get_lineage_dev"](unique_id="model.a", types=None, depth=5)

    mock_file.assert_called_once_with(
        "/test/project/target/manifest.json", encoding="utf-8"
    )
