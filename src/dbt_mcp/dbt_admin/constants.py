from enum import Enum


class JobRunStatus(str, Enum):
    """Enum for job run status values."""

    QUEUED = "queued"
    STARTING = "starting"
    RUNNING = "running"
    SUCCESS = "success"
    ERROR = "error"
    CANCELLED = "cancelled"


class RunResultsStatus(str, Enum):
    """Enum for run_results.json status values."""

    SUCCESS = "success"
    ERROR = "error"
    FAIL = "fail"
    SKIP = "skip"
    WARN = "warn"


class FreshnessStatus(str, Enum):
    """Enum for source freshness status values."""

    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


STATUS_MAP = {
    JobRunStatus.QUEUED: 1,
    JobRunStatus.STARTING: 2,
    JobRunStatus.RUNNING: 3,
    JobRunStatus.SUCCESS: 10,
    JobRunStatus.ERROR: 20,
    JobRunStatus.CANCELLED: 30,
}

TRUNCATED_LOGS_LENGTH = 50  # Number of log lines to keep when truncating logs
