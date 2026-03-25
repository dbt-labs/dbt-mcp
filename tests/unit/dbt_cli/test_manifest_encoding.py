"""Test that _get_manifest reads manifest.json with UTF-8 encoding."""

from unittest.mock import MagicMock, mock_open, patch

import pytest

from dbt_mcp.dbt_cli.tools import lineage_dev_tools


@patch("dbt_mcp.dbt_cli.tools._run_dbt_command")
@patch("builtins.open", new_callable=mock_open, read_data='{"nodes": {}}')
def test_get_manifest_uses_utf8_encoding(mock_file, mock_run_dbt, tmp_path):
    """Verify manifest.json is opened with encoding='utf-8'.

    Regression test for #594: on Windows, open() without encoding
    defaults to the system locale (CP-1252), which fails on valid
    UTF-8 characters in model descriptions.
    """
    from dbt_mcp.dbt_cli import config

    manifest_path = tmp_path / "target" / "manifest.json"
    manifest_path.parent.mkdir(parents=True)

    with patch.object(config, "project_dir", str(tmp_path)):
        lineage_dev_tools._get_manifest()

    mock_file.assert_called_once()
    call_args = mock_file.call_args
    assert call_args[0][0] == str(manifest_path) or "manifest.json" in call_args[0][0]
    assert call_args[1].get("encoding") == "utf-8"
