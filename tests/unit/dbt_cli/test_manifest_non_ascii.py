"""Test that _get_manifest handles non-ASCII UTF-8 in manifest.json.

Regression test for #594: on Windows, open() without encoding defaults
to the system locale (CP-1252), which fails on valid UTF-8 characters
in model descriptions like accented letters or CJK characters.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dbt_mcp.dbt_cli.tools import lineage_dev_tools


def _make_manifest_with_non_ascii():
    """Create a manifest.json containing non-ASCII UTF-8 characters."""
    return json.dumps(
        {
            "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v14.json"},
            "nodes": {
                "model.my_project.customers": {
                    "unique_id": "model.my_project.customers",
                    "description": "Table des clients — 注文追跡モデル",
                    "columns": {
                        "name": {"description": "Nom du client — 顧客名", "name": "name"},
                        "city": {"description": "Ville — 都市", "name": "city"},
                    },
                }
            },
        },
        ensure_ascii=False,
    )


@patch("dbt_mcp.dbt_cli.tools._run_dbt_command")
def test_get_manifest_handles_non_ascii_utf8(mock_run_dbt, tmp_path):
    """Verify manifest.json with accented/CJK characters loads correctly.

    Without encoding='utf-8', this raises UnicodeDecodeError on Windows.
    """
    from dbt_mcp.dbt_cli import config

    manifest_dir = tmp_path / "target"
    manifest_dir.mkdir()
    manifest_path = manifest_dir / "manifest.json"
    manifest_path.write_text(_make_manifest_with_non_ascii(), encoding="utf-8")

    # Mock _run_dbt_command to return the tmp_path as project dir
    mock_run_dbt.return_value = tmp_path

    with patch.object(config, "project_dir", str(tmp_path)):
        manifest = lineage_dev_tools._get_manifest()

    assert manifest is not None
    desc = manifest["nodes"]["model.my_project.customers"]["description"]
    assert "clients" in desc
    assert "注文" in desc
    assert "顧客名" in desc
