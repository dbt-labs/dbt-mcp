from __future__ import annotations
import sys
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


def configure_logging(
    *, file_logging: bool, log_level: str | int | None = None
) -> None:
    lg_lvl = logging.INFO if log_level is None else log_level
    root_logger = logging.getLogger()
    root_logger.setLevel(lg_lvl)

    # ensure stderr handler exists
    configure_stderr_logging(root_logger, lg_lvl)

    if file_logging:
        configure_file_logging(root_logger, lg_lvl)


def configure_stderr_logging(root_logger: logging.Logger, lg_lvl: str | int) -> None:
    """Configure stderr logging for the root logger."""
    for handler in root_logger.handlers:
        if isinstance(handler, logging.StreamHandler) and hasattr(handler, "stream"):
            if handler.stream is sys.stderr or (
                hasattr(handler.stream, "name") and handler.stream.name == "<stderr>"
            ):
                # update existing stderr handler's level
                handler.setLevel(lg_lvl)
                root_logger.info(f"Updated stderr handler level to {lg_lvl}")
                return

    # add stderr handler if not found
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(lg_lvl)
    stderr_handler.setFormatter(
        logging.Formatter("%(levelname)s [%(name)s] %(message)s")
    )
    root_logger.addHandler(stderr_handler)
    root_logger.info(f"added stderr handler with level {lg_lvl}")


def configure_file_logging(root_logger: logging.Logger, lg_lvl: str | int) -> None:
    """Configure file logging for the root logger."""
    repo_root = _find_repo_root()
    log_path = repo_root / LOG_FILENAME

    # Check if file handler already exists
    for handler in root_logger.handlers:
        if (
            isinstance(handler, logging.FileHandler)
            and Path(handler.baseFilename) == log_path
        ):
            # Update existing file handler's level
            handler.setLevel(lg_lvl)
            return

    # Add new file handler
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(lg_lvl)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )
    root_logger.addHandler(file_handler)
