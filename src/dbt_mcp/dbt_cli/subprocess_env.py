import os
from pathlib import Path


def get_dbt_subprocess_env(
    project_dir: str, profiles_dir: str | None = None
) -> dict[str, str]:
    """Environment for dbt subprocesses, with directory variables made absolute.

    The subprocess runs with `project_dir` as its working directory, so relative
    DBT_PROJECT_DIR / DBT_PROFILES_DIR values inherited from the MCP server's
    environment would be resolved a second time against that directory.
    """
    env = os.environ.copy()
    env["DBT_PROJECT_DIR"] = str(Path(project_dir).expanduser().resolve())
    if profiles_dir := profiles_dir or env.get("DBT_PROFILES_DIR"):
        env["DBT_PROFILES_DIR"] = str(Path(profiles_dir).expanduser().resolve())
    return env
