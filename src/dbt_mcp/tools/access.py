from dataclasses import dataclass
from enum import StrEnum


class ToolPermission(StrEnum):
    DEVELOP_ACCESS = "develop_access"
    METADATA_READ = "metadata_read"
    SEMANTIC_LAYER_CONFIGURATION_READ = "semantic_layer_configuration_read"
    PROJECTS_READ = "projects_read"
    JOBS_READ = "jobs_read"
    RUNS_READ = "runs_read"
    RUNS_WRITE = "runs_write"


class ToolTarget(StrEnum):
    PUBLIC = "public"
    LOCAL = "local"
    PRODUCTION_ENVIRONMENT = "production_environment"
    DEVELOPMENT_ENVIRONMENT = "development_environment"
    PROJECTS = "projects"
    JOBS = "jobs"
    JOB = "job"
    RUNS = "runs"
    RUN = "run"


@dataclass(frozen=True, kw_only=True)
class ToolAccess:
    """Server-side requirements, separate from client-facing MCP hints.

    Environment targets describe selection preferences. Hosts resolve and
    authorize the actual resources before executing tools; these declarations
    do not grant access or replace authorization by downstream services.
    Local targets use the host's local configuration and credentials, rather
    than selecting a cloud resource for authorization.
    """

    target: ToolTarget
    permissions: tuple[ToolPermission, ...] = ()
