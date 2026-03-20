import asyncio
import os
import sys
from pathlib import Path

from dbt_mcp.config.config import load_config
from dbt_mcp.config.transport import validate_transport
from dbt_mcp.mcp.server import create_dbt_mcp


def _load_env_file(path: str) -> None:
    """Load KEY=VALUE pairs into os.environ (same pattern as typical .env files)."""
    p = Path(path).expanduser()
    if not p.is_file():
        return
    for raw_line in p.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if value.startswith('"') and value.endswith('"') and len(value) >= 2:
            value = value[1:-1].replace('\\"', '"')
        elif value.startswith("'") and value.endswith("'") and len(value) >= 2:
            value = value[1:-1]
        if key:
            os.environ[key] = value


def _apply_env_file_cli_args() -> None:
    """Support `dbt-mcp --env-file /path/to.env` (args after `uv run dbt-mcp`).

    `uv run --env-file ... dbt-mcp` is preferred, but many MCP configs pass
    `--env-file` to the dbt-mcp process; we must load it before pydantic reads
    settings (otherwise only `.env` in the project dir is used).
    """
    new_argv: list[str] = [sys.argv[0]]
    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == "--env-file" and i + 1 < len(sys.argv):
            _load_env_file(sys.argv[i + 1])
            i += 2
            continue
        new_argv.append(sys.argv[i])
        i += 1
    sys.argv = new_argv


def main() -> None:
    _apply_env_file_cli_args()
    config = load_config()
    server = asyncio.run(create_dbt_mcp(config))
    transport = validate_transport(os.environ.get("MCP_TRANSPORT", "stdio"))
    server.run(transport=transport)


if __name__ == "__main__":
    main()
