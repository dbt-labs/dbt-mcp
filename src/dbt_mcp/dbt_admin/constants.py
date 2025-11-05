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


STATUS_MAP = {
    JobRunStatus.QUEUED: 1,
    JobRunStatus.STARTING: 2,
    JobRunStatus.RUNNING: 3,
    JobRunStatus.SUCCESS: 10,
    JobRunStatus.ERROR: 20,
    JobRunStatus.CANCELLED: 30,
}

# String match in run_results_errors/parser.py to identify source freshness step
# in run_details response
SOURCE_FRESHNESS_STEP_NAME = "source freshness"
