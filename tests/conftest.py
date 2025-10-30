from pathlib import Path
import pytest
from contextlib import contextmanager
import os


@pytest.fixture
def env_setup(tmp_path: Path, monkeypatch):
    """
    Returns a helper that creates a temporary project layout and applies env vars.
    Needed so the MCP doesn't auto-disable tools due to bad validations.

    Usage:
        project_dir, helpers = env_setup()
        project_dir, helpers = env_setup(env_vars={"DBT_HOST": "host"}, files={"models/foo.sql": "select 1"})
        # or:
        project_dir, helpers = env_setup()
        helpers.set_env({"DBT_HOST": "host"})
        helpers.write_file("models/foo.sql", "select 1")

    The monkeypatch ensures env vars are removed at test teardown.
    """

    @contextmanager
    def _make(
        env_vars: dict[str, str] | None = None, files: dict[str, str] | None = None
    ):
        project_dir = tmp_path / "project"
        project_dir.mkdir()

        # a placeholder dbt file (tests expect a path called 'dbt')
        dbt_path = tmp_path / "dbt"
        dbt_path.touch()

        path_plus_tmp_path = os.environ.get("PATH", "") + os.pathsep + str(tmp_path)
        default_env_vars = {
            "DBT_PROJECT_DIR": str(
                project_dir
            ),  # so cli doesn't get disabled automatically
            "PATH": path_plus_tmp_path,  # so it can find fake `dbt` that we created
            "DBT_HOST": "http://localhost:8000",  # so platform doesn't get auto-disabled
        }

        env_vars = default_env_vars | (env_vars or {})

        class Helpers:
            @staticmethod
            def set_env(mapping: dict[str, str]):
                for k, v in mapping.items():
                    monkeypatch.setenv(k, v)

            @staticmethod
            def unset_env(*names: str):
                for n in names:
                    # ensure removal; monkeypatch doesn't have a direct unset method but setenv(None) isn't supported
                    # so set to empty string or use patch.dict in tests if they need `clear=True`.
                    monkeypatch.delenv(n, raising=False)

            @staticmethod
            def write_file(relpath: str, content: str):
                p = project_dir / relpath
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(content)
                return p

            @staticmethod
            def path(relpath: str):
                return project_dir / relpath

        helpers = Helpers()

        helpers.set_env(env_vars)
        if files:
            for rel, content in files.items():
                helpers.write_file(rel, content)

        yield project_dir, helpers

    yield _make
