import os
from pathlib import Path

import pytest

from dbt_mcp.config.settings import DbtMcpSettings
from dbt_mcp.dbt_cli.subprocess_env import get_dbt_subprocess_env


def _load_settings(env_file: Path | None = None) -> DbtMcpSettings:
    # pydantic-settings accepts `_env_file` at runtime but mypy can't see it
    return DbtMcpSettings(_env_file=env_file)  # type: ignore[call-arg]


@pytest.fixture
def workdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    (tmp_path / "transform").mkdir()
    (tmp_path / "profiles").mkdir()
    (tmp_path / "bin").mkdir()
    dbt = tmp_path / "bin" / "dbt"
    dbt.touch()
    dbt.chmod(0o755)
    for var in ("DBT_PROJECT_DIR", "DBT_PROFILES_DIR", "DBT_PATH"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_relative_dbt_project_dir_is_made_absolute(
    workdir: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DBT_PROJECT_DIR", "./transform")
    settings = _load_settings()
    assert settings.dbt_project_dir == str((workdir / "transform").resolve())


def test_relative_dbt_profiles_dir_is_made_absolute(
    workdir: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DBT_PROFILES_DIR", "profiles")
    settings = _load_settings()
    assert settings.dbt_profiles_dir == str((workdir / "profiles").resolve())


def test_relative_dbt_path_is_made_absolute(
    workdir: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DBT_PATH", "bin/dbt")
    settings = _load_settings()
    assert settings.dbt_path == str((workdir / "bin" / "dbt").resolve())


def test_bare_dbt_path_is_left_for_path_lookup(
    workdir: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DBT_PATH", "dbt")
    settings = _load_settings()
    assert settings.dbt_path == "dbt"


def test_relative_paths_in_env_file_resolve_against_cwd(workdir: Path):
    env_file = workdir / ".env"
    env_file.write_text("DBT_PROJECT_DIR=./transform\nDBT_PATH=bin/dbt\n")
    settings = _load_settings(env_file)
    assert settings.dbt_project_dir == str((workdir / "transform").resolve())
    assert settings.dbt_path == str((workdir / "bin" / "dbt").resolve())


def test_subprocess_env_overrides_inherited_relative_directories(
    workdir: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DBT_PROJECT_DIR", "./transform")
    monkeypatch.setenv("DBT_PROFILES_DIR", "profiles")
    project_dir = str((workdir / "transform").resolve())
    profiles_dir = str((workdir / "profiles").resolve())
    env = get_dbt_subprocess_env(project_dir, profiles_dir)
    assert env["DBT_PROJECT_DIR"] == project_dir
    assert env["DBT_PROFILES_DIR"] == profiles_dir


def test_subprocess_env_uses_profiles_dir_from_env_file(
    workdir: Path, monkeypatch: pytest.MonkeyPatch
):
    (workdir / ".env").write_text("DBT_PROFILES_DIR=profiles\n")
    settings = _load_settings(workdir / ".env")
    assert "DBT_PROFILES_DIR" not in os.environ
    env = get_dbt_subprocess_env(str(workdir / "transform"), settings.dbt_profiles_dir)
    assert env["DBT_PROFILES_DIR"] == str((workdir / "profiles").resolve())


def test_subprocess_env_resolves_relative_arguments_and_inherited_profiles_dir(
    workdir: Path, monkeypatch: pytest.MonkeyPatch
):
    env = get_dbt_subprocess_env("transform")
    assert env["DBT_PROJECT_DIR"] == str((workdir / "transform").resolve())

    monkeypatch.setenv("DBT_PROFILES_DIR", "profiles")
    env = get_dbt_subprocess_env("transform")
    assert env["DBT_PROFILES_DIR"] == str((workdir / "profiles").resolve())

    env = get_dbt_subprocess_env("transform", "other")
    assert env["DBT_PROFILES_DIR"] == str((workdir / "other").resolve())


def test_subprocess_env_does_not_add_profiles_dir(workdir: Path):
    env = get_dbt_subprocess_env(str(workdir / "transform"))
    assert "DBT_PROFILES_DIR" not in env
