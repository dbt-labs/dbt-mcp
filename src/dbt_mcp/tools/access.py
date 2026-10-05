from enum import Enum


class AccessPolicy(Enum):
    """Additional policies implemented by the host, alongside target annotations."""

    LOCAL = "local"
    JOBS_READ = "jobs_read"
    RUNS_READ = "runs_read"
    PRODUCTION_SEMANTIC_LAYER_CONFIGURATION_READ = (
        "production_semantic_layer_configuration_read"
    )
