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
    log_level = logging.INFO if log_level is None else log_level
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # ensure stderr handler exists
    configure_stderr_logging(root_logger, log_level)

    if file_logging:
        configure_file_logging(root_logger, log_level)


def configure_stderr_logging(root_logger: logging.Logger, log_level: str | int) -> None:
    """Configure stderr logging for the root logger."""
    for handler in root_logger.handlers:
        if isinstance(handler, logging.StreamHandler) and hasattr(handler, "stream"):
            if handler.stream is sys.stderr or (
                hasattr(handler.stream, "name") and handler.stream.name == "<stderr>"
            ):
                # update existing stderr handler's level
                handler.setLevel(log_level)
                root_logger.debug(f"Updated stderr handler level to {log_level}")
                return

    # add stderr handler if not found
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(log_level)
    stderr_handler.setFormatter(
        logging.Formatter("%(levelname)s [%(name)s] %(message)s")
    )
    root_logger.addHandler(stderr_handler)
    root_logger.debug(f"Added stderr handler with level {log_level}")


def configure_file_logging(root_logger: logging.Logger, log_level: str | int) -> None:
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
            handler.setLevel(log_level)
            return

    # Add new file handler
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(log_level)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )
    root_logger.addHandler(file_handler)
