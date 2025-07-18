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
