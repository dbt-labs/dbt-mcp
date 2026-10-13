from dataclasses import dataclass
from enum import Enum, StrEnum
from inspect import Signature, signature
from typing import Annotated, Any, get_args, get_origin
from collections.abc import Callable, Iterable


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
    # The host resolves this environment alongside the project before invocation.
    environment: EnvironmentRole | None = None


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


def target_environment_role(targets: Iterable[Target]) -> EnvironmentRole | None:
    roles = set()
    for target in targets:
        role = (
            target.environment
            if isinstance(target, ProjectTarget)
            else target.role
            if isinstance(target, EnvironmentTarget)
            else None
        )
        if role is not None:
            if not isinstance(role, EnvironmentRole):
                raise ValueError(f"Unsupported target environment: {role!r}")
            roles.add(role)
    if len(roles) > 1:
        raise ValueError("Additional environment checks require a custom policy")
    return next(iter(roles), None)


def target_parameters(fn: Callable[..., Any] | Signature) -> dict[str, Target]:
    targets = {}
    parameters = (fn if isinstance(fn, Signature) else signature(fn)).parameters
    for name, parameter in parameters.items():
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
