import logging
import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from dbt_mcp.config.settings import (
    DbtMcpSettings,
    split_command,
    validate_dbt_cli_settings,
)
from dbt_mcp.dbt_cli.binary_type import BinaryType


def _load_settings(env: dict[str, str]) -> DbtMcpSettings:
    with patch.dict(os.environ, env, clear=True):
        # pydantic-settings accepts `_env_file` at runtime but mypy can't see it
        return DbtMcpSettings(_env_file=None)  # type: ignore[call-arg]


def test_split_command_posix() -> None:
    assert split_command('"/opt/my tools/python" -m dbt.cli.main', windows=False) == [
        "/opt/my tools/python",
        "-m",
        "dbt.cli.main",
    ]


def test_split_command_windows_keeps_backslashes() -> None:
    command = r"C:\Users\me\project\.venv\Scripts\python.exe -m dbt.cli.main"
    assert split_command(command, windows=True) == [
        r"C:\Users\me\project\.venv\Scripts\python.exe",
        "-m",
        "dbt.cli.main",
    ]


def test_split_command_windows_strips_quotes() -> None:
    command = r'"C:\Program Files\Python\python.exe" -m dbt.cli.main'
    assert split_command(command, windows=True) == [
        r"C:\Program Files\Python\python.exe",
        "-m",
        "dbt.cli.main",
    ]


def test_dbt_command_args_falls_back_to_dbt_path() -> None:
    settings = _load_settings({"DBT_PATH": "dbt"})
    assert settings.dbt_command is None
    assert settings.dbt_command_args == ["dbt"]


def test_dbt_command_args_uses_dbt_command() -> None:
    settings = _load_settings(
        {"DBT_PATH": "dbt", "DBT_COMMAND": "python -m dbt.cli.main"}
    )
    assert settings.dbt_command_args == ["python", "-m", "dbt.cli.main"]


def test_blank_dbt_command_is_ignored() -> None:
    settings = _load_settings({"DBT_COMMAND": "   "})
    assert settings.dbt_command is None
    assert settings.dbt_command_args == ["dbt"]


def test_warns_when_dbt_command_and_dbt_path_are_both_set(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger="dbt_mcp.config.settings"):
        _load_settings({"DBT_PATH": "dbt", "DBT_COMMAND": "python -m dbt.cli.main"})
    assert "Both DBT_COMMAND and DBT_PATH are set" in caplog.text


def test_no_warning_when_only_dbt_command_is_set(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger="dbt_mcp.config.settings"):
        _load_settings({"DBT_COMMAND": "python -m dbt.cli.main"})
    assert "Both DBT_COMMAND and DBT_PATH are set" not in caplog.text


def test_unparseable_dbt_command_fails() -> None:
    with pytest.raises(ValidationError, match="DBT_COMMAND could not be parsed"):
        _load_settings({"DBT_COMMAND": 'python -m "dbt.cli.main'})


def test_dbt_command_with_existing_executable_keeps_cli_enabled(
    tmp_path: Path,
) -> None:
    python = tmp_path / "python"
    python.touch()
    settings = _load_settings(
        {
            "DBT_PROJECT_DIR": str(tmp_path),
            "DBT_COMMAND": f"{python} -m dbt.cli.main",
        }
    )
    assert validate_dbt_cli_settings(settings) == []
    assert settings.disable_dbt_cli is False


def test_dbt_command_with_missing_executable_disables_cli(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    settings = _load_settings(
        {
            "DBT_PROJECT_DIR": str(tmp_path),
            "DBT_COMMAND": f"{missing} -m dbt.cli.main",
        }
    )
    # the settings model turns CLI tools off when validation fails
    assert settings.disable_dbt_cli is True
    settings.disable_dbt_cli = False
    assert validate_dbt_cli_settings(settings) == [
        f"DBT_COMMAND executable can't be found: {missing}"
    ]


def test_detect_binary_type_runs_full_command() -> None:
    from dbt_mcp.dbt_cli import binary_type

    # conftest stubs detect_binary_type, so use the saved real one
    detect = binary_type._real_detect_binary_type  # type: ignore[attr-defined]
    result = MagicMock(stdout="Usage: dbt [OPTIONS] COMMAND [ARGS]...\n")
    with patch.object(subprocess, "run", return_value=result) as mock_run:
        detected = detect(["python", "-m", "dbt.cli.main"])
    assert mock_run.call_args.args[0] == ["python", "-m", "dbt.cli.main", "--help"]
    assert detected == BinaryType.DBT_CORE
