"""Tests for dbt-mcp --env-file CLI handling."""

import os
import subprocess
import sys
from pathlib import Path

import pytest


def test_apply_env_file_cli_loads_vars(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_path = tmp_path / "custom.env"
    env_path.write_text("DBT_MCP_ENABLE_CODE_MODE=true\nDBT_FOO=bar\n", encoding="utf-8")

    monkeypatch.setattr(sys, "argv", ["dbt-mcp", "--env-file", str(env_path)])
    from dbt_mcp import main as main_mod

    main_mod._apply_env_file_cli_args()

    assert sys.argv == ["dbt-mcp"]
    assert os.environ.get("DBT_MCP_ENABLE_CODE_MODE") == "true"
    assert os.environ.get("DBT_FOO") == "bar"


def test_subprocess_dbt_mcp_env_file_sets_code_mode(tmp_path: Path) -> None:
    """Integration: main() loads --env-file before load_config (may fail on missing deps)."""
    env_path = tmp_path / "m.env"
    env_path.write_text(
        "DBT_MCP_ENABLE_CODE_MODE=true\n"
        "DISABLE_DBT_CLI=true\n"
        "DISABLE_DBT_CODEGEN=true\n",
        encoding="utf-8",
    )
    repo_root = Path(__file__).resolve().parents[2]
    code = (
        "import os\n"
        "import sys\n"
        "sys.path.insert(0, str(%r))\n"
        "from dbt_mcp.main import _apply_env_file_cli_args\n"
        "sys.argv = ['dbt-mcp', '--env-file', %r]\n"
        "_apply_env_file_cli_args()\n"
        "from dbt_mcp.config.settings import DbtMcpSettings\n"
        "s = DbtMcpSettings(_env_file=None)\n"
        "print('OK' if s.enable_code_mode else 'FAIL')\n"
    ) % (str(repo_root / "src"), str(env_path))
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert completed.stdout.strip() == "OK", completed.stderr + completed.stdout
