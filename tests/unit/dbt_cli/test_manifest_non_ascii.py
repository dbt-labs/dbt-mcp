import json

import pytest
from pytest import MonkeyPatch

from dbt_mcp.config.config import DbtCliConfig
from dbt_mcp.dbt_cli.binary_type import BinaryType
from dbt_mcp.dbt_cli.tools import register_dbt_cli_tools


@pytest.fixture
def mock_process():
    class MockProcess:
        returncode = 0

        def communicate(self, timeout=None):
            return "command output", ""

    return MockProcess()


def test_manifest_loads_non_ascii_utf8(
    monkeypatch: MonkeyPatch, mock_process, mock_fastmcp, tmp_path
):
    """Regression test for #594: manifest with non-ASCII UTF-8 chars should load without error."""
    manifest_dir = tmp_path / "target"
    manifest_dir.mkdir()
    manifest_data = {
        "nodes": {
            "model.my_project.customers": {
                "name": "customers",
                "description": "Área México — 注文追跡モデル",
            }
        }
    }
    (manifest_dir / "manifest.json").write_text(
        json.dumps(manifest_data, ensure_ascii=False), encoding="utf-8"
    )

    mock_calls = []

    def mock_popen(args, **kwargs):
        mock_calls.append(args)
        return mock_process

    monkeypatch.setattr("subprocess.Popen", mock_popen)

    config = DbtCliConfig(
        project_dir=str(tmp_path),
        dbt_path="/path/to/dbt",
        dbt_cli_timeout=10,
        binary_type=BinaryType.DBT_CORE,
    )

    fastmcp, tools = mock_fastmcp
    register_dbt_cli_tools(
        fastmcp,
        config,
        disabled_tools=set(),
        enabled_tools=None,
        enabled_toolsets=set(),
        disabled_toolsets=set(),
    )

    tools["get_lineage_dev"](
        unique_id="model.my_project.customers", types=None, depth=5
    )
