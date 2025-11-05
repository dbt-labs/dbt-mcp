from __future__ import annotations

import logging
from pathlib import Path

LOG_FILENAME = "dbt-mcp.log"


def _find_repo_root() -> Path:
    module_path = Path(__file__).resolve().parent
    home = Path.home().resolve()
    for candidate in [module_path, *module_path.parents]:
        if (
            (candidate / ".git").exists()
            or (candidate / "pyproject.toml").exists()
            or candidate == home
        ):
            return candidate
    return module_path


def configure_logging(file_logging: bool) -> None:
    if not file_logging:
        return

    repo_root = _find_repo_root()
    log_path = repo_root / LOG_FILENAME

    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if (
            isinstance(handler, logging.FileHandler)
            and Path(handler.baseFilename) == log_path
        ):
            return

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )

    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
