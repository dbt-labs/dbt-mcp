import asyncio
import sys
import tempfile
from contextlib import suppress
from pathlib import Path

import httpx

from dbt_mcp.errors import (
    ArtifactRetrievalError,
    InvalidParameterError,
    ResponseLimitError,
)
from dbt_mcp.http_limits import response_limit_hook
from dbt_mcp.resource_limits import ArtifactConfig, ResponseLimits


class InlineArtifactLimitError(InvalidParameterError):
    """The artifact needs a filter before it can be returned inline."""


async def read_artifact(
    url: str,
    *,
    headers: dict[str, str],
    params: dict[str, int],
    timeout: float,
    jq_filter: str | None,
    config: ArtifactConfig = ArtifactConfig(),
    response_type: str = "artifact.other",
) -> str:
    if jq_filter is not None and len(jq_filter) > config.filter_chars:
        raise InvalidParameterError(
            f"jq_filter must contain at most {config.filter_chars} characters."
        )
    limits = (
        config.response_limits
        if jq_filter is not None
        else ResponseLimits(config.response_limits.wire_bytes, config.inline_bytes)
    )
    async with config.admission(), asyncio.timeout(config.execution_seconds):
        with tempfile.TemporaryDirectory(prefix="dbt-mcp-artifact-") as directory:
            path = Path(directory) / "artifact"
            try:
                async with httpx.AsyncClient(
                    timeout=timeout,
                    event_hooks={
                        "response": [
                            response_limit_hook(
                                limits,
                                response_type=response_type,
                                observer=config.observer,
                            )
                        ]
                    },
                ) as client:
                    async with client.stream(
                        "GET",
                        url,
                        headers=headers | {"Accept-Encoding": "gzip, deflate"},
                        params=params,
                    ) as response:
                        response.raise_for_status()
                        with path.open("wb") as target:
                            async for chunk in response.aiter_bytes():
                                target.write(chunk)
            except ResponseLimitError as error:
                if jq_filter is None and "decoded size limit" in str(error):
                    raise InlineArtifactLimitError(
                        "Artifact exceeds the inline size limit; use jq_filter."
                    ) from error
                raise
            with path.open("rb") as source:
                if source.read(4) == b"PAR1":
                    raise ArtifactRetrievalError(
                        "binary Parquet artifacts cannot be returned as text."
                    )
            if jq_filter is None:
                return path.read_text(encoding="utf-8", errors="replace")
            return await _filter_artifact(path, jq_filter, config)


async def _filter_artifact(path: Path, jq_filter: str, config: ArtifactConfig) -> str:
    launch = asyncio.create_task(
        asyncio.create_subprocess_exec(
            sys.executable,
            str(Path(__file__).with_name("artifact_worker.py")),
            str(path),
            jq_filter,
            str(config.worker_memory_bytes)
            if config.worker_memory_bytes is not None
            else "",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=64 * 1024,
        )
    )
    try:
        process = await asyncio.shield(launch)
    except asyncio.CancelledError:
        # A cancelled launch can still create a child. Finish spawning before
        # killing it so no worker outlives the capacity slot or temporary file.
        while not launch.done():
            with suppress(asyncio.CancelledError):
                await asyncio.shield(launch)
        await _stop_worker(launch.result())
        raise
    assert process.stdout is not None and process.stderr is not None
    result = bytearray()
    try:
        while chunk := await process.stdout.read(64 * 1024):
            if len(result) + len(chunk) >= config.output_bytes:
                raise InvalidParameterError(
                    f"Filtered output exceeds {config.output_bytes // 1024} KiB; narrow the filter to return fewer results."
                )
            result.extend(chunk)
        error = await process.stderr.read(8192)
        status = await process.wait()
        if status:
            if status == 2:
                raise InvalidParameterError(error.decode("utf-8", errors="replace"))
            raise InvalidParameterError(
                "Artifact worker resource limit exceeded; select a smaller artifact or step."
            )
        return result.decode("utf-8")
    finally:
        await _stop_worker(process)


async def _stop_worker(process: asyncio.subprocess.Process) -> None:
    if process.returncode is None:
        with suppress(ProcessLookupError):
            process.kill()
    reap = asyncio.create_task(process.wait())
    cancelled = False
    while not reap.done():
        try:
            await asyncio.shield(reap)
        except asyncio.CancelledError:
            cancelled = True
    reap.result()
    if cancelled:
        raise asyncio.CancelledError
