from enum import Enum


class AccessPolicy(Enum):
    """Additional policies implemented by the host, alongside target annotations."""

    LOCAL = "local"
    # Listing checks the selected scope, falling back to the account when unfiltered.
    JOBS_LISTING = "jobs_listing"
    RUNS_LISTING = "runs_listing"
