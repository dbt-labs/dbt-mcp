from dataclasses import dataclass
from enum import Enum, StrEnum
from inspect import signature
from typing import Annotated, Any, get_args, get_origin
from collections.abc import Callable


class Permission(Enum):
    METADATA_READ = "metadata_read"
    SEMANTIC_LAYER_CONFIGURATION_READ = "semantic_layer_configuration_read"
    DEVELOP_ACCESS = "develop_access"
    PROJECTS_READ = "projects_read"
    JOBS_READ = "jobs_read"
    JOBS_WRITE = "jobs_write"
    RUNS_READ = "runs_read"
    RUNS_WRITE = "runs_write"


class EnvironmentRole(StrEnum):
    DEVELOPMENT = "development"
    PRODUCTION = "production"


@dataclass(frozen=True)
class Target:
    requires: Permission


@dataclass(frozen=True)
class ProjectTarget(Target):
    pass


@dataclass(frozen=True)
class AccountTarget(Target):
    pass


@dataclass(frozen=True)
class EnvironmentTarget(Target):
    role: EnvironmentRole


@dataclass(frozen=True)
class JobTarget(Target):
    pass


@dataclass(frozen=True)
class RunTarget(Target):
    pass


def target_parameters(fn: Callable[..., Any]) -> dict[str, Target]:
    targets = {}
    for name, parameter in signature(fn).parameters.items():
        if get_origin(parameter.annotation) is not Annotated:
            continue
        declarations = [
            value
            for value in get_args(parameter.annotation)[1:]
            if isinstance(value, Target)
        ]
        if len(declarations) > 1:
            raise ValueError(f"Multiple target declarations for {name}")
        if declarations:
            targets[name] = declarations[0]
    return targets
