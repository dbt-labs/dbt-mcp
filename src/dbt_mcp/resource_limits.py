"""Per-client resource budgets and optional host admission hooks."""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, nullcontext
from dataclasses import dataclass
from math import isfinite

from dbt_mcp.errors import ConfigurationError

_KiB = 1024
_MiB = 1024 * _KiB

Admission = Callable[[], AbstractAsyncContextManager[None]]


def unrestricted_admission() -> AbstractAsyncContextManager[None]:
    return nullcontext()


@dataclass(frozen=True)
class ResponseLimits:
    wire_bytes: int
    decoded_bytes: int

    def __post_init__(self) -> None:
        if self.wire_bytes < 1 or self.decoded_bytes < 1:
            raise ConfigurationError("Response byte budgets must be positive.")


@dataclass(frozen=True)
class HttpConfig:
    response_limits: ResponseLimits = ResponseLimits(4 * _MiB, 2 * _MiB)
    admission: Admission = unrestricted_admission


@dataclass(frozen=True)
class ArtifactConfig:
    response_limits: ResponseLimits = ResponseLimits(16 * _MiB, 32 * _MiB)
    inline_bytes: int = 500 * _KiB
    output_bytes: int = 500 * _KiB
    worker_memory_bytes: int = 256 * _MiB
    execution_seconds: float = 120
    filter_chars: int = 8192
    admission: Admission = unrestricted_admission

    def __post_init__(self) -> None:
        if (
            min(
                self.inline_bytes,
                self.output_bytes,
                self.worker_memory_bytes,
                self.filter_chars,
            )
            < 1
        ):
            raise ConfigurationError("Artifact budgets must be positive.")
        if not isfinite(self.execution_seconds) or self.execution_seconds <= 0:
            raise ConfigurationError(
                "Artifact execution budget must be finite and positive."
            )


@dataclass(frozen=True)
class ProductDocsConfig:
    cache_bytes: int = 32 * _MiB
    cache_entries: int = 128
    index_limits: ResponseLimits = ResponseLimits(16 * _MiB, 16 * _MiB)

    def __post_init__(self) -> None:
        if self.cache_bytes < 1 or self.cache_entries < 1:
            raise ConfigurationError("Documentation cache budgets must be positive.")
