import os
from contextlib import contextmanager


@contextmanager
def env_vars_context(env_vars: dict[str, str]):
    """Temporarily set environment variables and restore them afterward."""
    # Store original env vars
    original_env = {}

    # Save original and set new values
    for key, value in env_vars.items():
        if key in os.environ:
            original_env[key] = os.environ[key]
        os.environ[key] = value

    try:
        yield
    finally:
        # Restore original values
        for key in env_vars:
            if key in original_env:
                os.environ[key] = original_env[key]
            else:
                del os.environ[key]


@contextmanager
def default_env_vars_context(override_env_vars: dict[str, str] | None = None):
    with env_vars_context(
        {
            "DBT_HOST": "http://localhost:8000",
            "DBT_PROD_ENV_ID": "1234",
            "DBT_TOKEN": "5678",
            "DBT_PROJECT_DIR": "tests/fixtures/dbt_project",
            "DBT_PATH": "dbt",
            "DBT_DEV_ENV_ID": "5678",
            "DBT_USER_ID": "9012",
            "DBT_CLI_TIMEOUT": "10",
            "DBT_ACCOUNT_ID": "12345",
            "DISABLE_TOOLS": "",
        }
        | (override_env_vars or {})
    ):
        yield
