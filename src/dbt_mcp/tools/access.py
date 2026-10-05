from enum import Enum


class AccessPolicy(Enum):
    """Named server-side policies, separate from client-facing MCP hints.

    Hosts implement resource selection and authorization for these policies.
    Declarations do not grant access or replace downstream authorization.
    The tool framework also accepts host-defined enums and preserves them
    without interpreting their values.
    """

    PUBLIC = "public"
    LOCAL = "local"
    ARGUMENTS = "arguments"
    PRODUCTION_METADATA_READ = "production_metadata_read"
    PRODUCTION_SEMANTIC_LAYER_CONFIGURATION_READ = (
        "production_semantic_layer_configuration_read"
    )
    ACCOUNT_PROJECTS_READ = "account_projects_read"
    JOBS_READ = "jobs_read"
    JOB_READ = "job_read"
    JOB_RUN_WRITE = "job_run_write"
    RUNS_READ = "runs_read"
    RUN_READ = "run_read"
    RUN_WRITE = "run_write"
