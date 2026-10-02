"""Per-client resource budgets and optional host admission hooks."""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, nullcontext
from dataclasses import dataclass
from math import isfinite

from dbt_mcp.errors import ConfigurationError

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
    response_limits: ResponseLimits = ResponseLimits(4 * 1024 * 1024, 2 * 1024 * 1024)
    admission: Admission = unrestricted_admission


@dataclass(frozen=True)
class ArtifactConfig:
    response_limits: ResponseLimits = ResponseLimits(16 * 1024 * 1024, 32 * 1024 * 1024)
    inline_bytes: int = 500 * 1024
    output_bytes: int = 500 * 1024
    worker_memory_bytes: int = 256 * 1024 * 1024
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
    cache_bytes: int = 32 * 1024 * 1024
    cache_entries: int = 128
    index_limits: ResponseLimits = ResponseLimits(16 * 1024 * 1024, 16 * 1024 * 1024)

    def __post_init__(self) -> None:
        if self.cache_bytes < 1 or self.cache_entries < 1:
            raise ConfigurationError("Documentation cache budgets must be positive.")
