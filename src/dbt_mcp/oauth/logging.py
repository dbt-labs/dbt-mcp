import logging


def disable_server_logs() -> None:
    # Disable uvicorn, fastapi, and related loggers
    loggers = (
        "uvicorn",
        "uvicorn.error",
        "uvicorn.access",
        "fastapi",
    )

    for logger_name in loggers:
        logging.getLogger(logger_name).disabled = True
