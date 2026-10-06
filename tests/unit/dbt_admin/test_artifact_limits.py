import asyncio
import gzip
from pathlib import Path
from contextlib import asynccontextmanager
from dataclasses import replace
from unittest.mock import patch

import httpx
import pytest
from dbt_mcp.config.config_providers import AdminApiConfig
from tests.unit.dbt_admin.test_client import MockHeadersProvider

from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.errors import InvalidParameterError, ResponseLimitError
from dbt_mcp.resource_limits import ArtifactConfig, ResponseLimits
from tests.unit.dbt_admin.test_client import (
    MockAdminApiConfigProvider,
)


@pytest.fixture
def admin_config():
    return AdminApiConfig(
        account_id=12345,
        url="https://cloud.getdbt.com",
        headers_provider=MockHeadersProvider({"Authorization": "Bearer test_token"}),
    )


class Chunks(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]):
        self.chunks = chunks
        self.read_count = 0
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            self.read_count += 1
            yield chunk

    async def aclose(self):
        self.closed = True


@pytest.mark.parametrize("encoding", ["identity", "gzip"])
@pytest.mark.parametrize("jq_filter", [None, "length"])
async def test_artifact_source_limit_stops_stream_before_eof(
    admin_config, encoding, jq_filter
):
    admin_config = replace(
        admin_config,
        artifact_config=ArtifactConfig(
            response_limits=ResponseLimits(16 * 1024 * 1024, 32 * 1024 * 1024)
        ),
    )
    payload = b"x" * (1024 * 1024)
    chunks = [payload] * 40
    if encoding == "gzip":
        chunks = [gzip.compress(b"".join(chunks)), b"unused"]
    stream = Chunks(chunks)
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, headers={"Content-Encoding": encoding}, stream=stream
        )
    )
    real_client = httpx.AsyncClient
    client = DbtAdminAPIClient(MockAdminApiConfigProvider(admin_config))
    with patch(
        "httpx.AsyncClient",
        side_effect=lambda **kw: real_client(transport=transport, **kw),
    ):
        with pytest.raises((InvalidParameterError, ResponseLimitError), match="limit"):
            await client.get_job_run_artifact(
                12345, 100, "manifest.json", jq_filter=jq_filter
            )
    assert stream.read_count < len(chunks)
    assert stream.closed


async def test_artifact_cancellation_closes_stream_and_releases_capacity(admin_config):
    started = asyncio.Event()
    release = asyncio.Event()

    class WaitingStream(Chunks):
        async def __aiter__(self):
            started.set()
            await release.wait()
            yield b"{}"

    stream = WaitingStream([])
    transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=stream))
    real_client = httpx.AsyncClient
    client = DbtAdminAPIClient(MockAdminApiConfigProvider(admin_config))
    with patch(
        "httpx.AsyncClient",
        side_effect=lambda **kw: real_client(transport=transport, **kw),
    ):
        task = asyncio.create_task(
            client.get_job_run_artifact(12345, 100, "manifest.json")
        )

        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stream.closed
        release.set()
        assert (
            await asyncio.wait_for(
                client.get_job_run_artifact(12345, 100, "manifest.json"), 2
            )
            == "{}"
        )


async def test_artifact_admission_is_shared_before_download(admin_config):
    started = asyncio.Event()
    release = asyncio.Event()
    requests = []

    class WaitingStream(Chunks):
        async def __aiter__(self):
            started.set()
            await release.wait()
            yield b"{}"

    def response(request):
        requests.append(request)
        return httpx.Response(200, stream=WaitingStream([]))

    transport = httpx.MockTransport(response)
    real_client = httpx.AsyncClient
    semaphore = asyncio.Semaphore(1)

    @asynccontextmanager
    async def admission():
        async with semaphore:
            yield

    config = replace(admin_config, artifact_config=ArtifactConfig(admission=admission))
    clients = [DbtAdminAPIClient(MockAdminApiConfigProvider(config)) for _ in range(3)]
    with patch(
        "httpx.AsyncClient",
        side_effect=lambda **kw: real_client(transport=transport, **kw),
    ):
        tasks = [
            asyncio.create_task(
                client.get_job_run_artifact(12345, 100, "manifest.json")
            )
            for client in clients[:2]
        ]
        try:
            await started.wait()
            await asyncio.sleep(0.05)
            assert len(requests) == 1
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        release.set()
        assert (
            await clients[2].get_job_run_artifact(12345, 100, "manifest.json") == "{}"
        )


async def test_artifact_admission_queues_a_25_call_burst_before_download(admin_config):
    started = asyncio.Event()
    release = asyncio.Event()
    requests = []

    class WaitingStream(Chunks):
        async def __aiter__(self):
            started.set()
            await release.wait()
            yield b"{}"

    def response(request):
        requests.append(request)
        return httpx.Response(200, stream=WaitingStream([]))

    real_client = httpx.AsyncClient
    semaphore = asyncio.Semaphore(1)

    @asynccontextmanager
    async def admission():
        async with semaphore:
            yield

    config = replace(admin_config, artifact_config=ArtifactConfig(admission=admission))
    client = DbtAdminAPIClient(MockAdminApiConfigProvider(config))
    with patch(
        "httpx.AsyncClient",
        side_effect=lambda **kw: real_client(
            transport=httpx.MockTransport(response), **kw
        ),
    ):
        tasks = [
            asyncio.create_task(client.get_job_run_artifact(12345, i, "manifest.json"))
            for i in range(25)
        ]
        try:
            await asyncio.wait_for(started.wait(), 1)
            await asyncio.sleep(0.05)
            assert len(requests) == 1
            assert all(not task.done() for task in tasks)
            release.set()
            assert await asyncio.wait_for(asyncio.gather(*tasks), 3) == ["{}"] * 25
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_cancellation_during_worker_launch_reaps_child_and_removes_spool(
    admin_config,
):
    started = asyncio.Event()
    release = asyncio.Event()
    processes = []
    paths = []
    launch = asyncio.create_subprocess_exec

    async def delayed_launch(*args, **kwargs):
        process = await launch(*args, **kwargs)
        processes.append(process)
        paths.append(Path(args[2]))
        started.set()
        await release.wait()
        return process

    transport = httpx.MockTransport(lambda request: httpx.Response(200, content=b"{}"))
    real_client = httpx.AsyncClient
    client = DbtAdminAPIClient(MockAdminApiConfigProvider(admin_config))
    with (
        patch(
            "httpx.AsyncClient",
            side_effect=lambda **kw: real_client(transport=transport, **kw),
        ),
        patch("asyncio.create_subprocess_exec", side_effect=delayed_launch),
    ):
        task = asyncio.create_task(
            client.get_job_run_artifact(
                12345, 100, "manifest.json", jq_filter="until(false; .)"
            )
        )
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        assert processes[0].returncode is not None
        assert not paths[0].parent.exists()
        assert (
            await client.get_job_run_artifact(
                12345, 100, "manifest.json", jq_filter="length"
            )
            == "[0]"
        )


async def test_default_artifact_config_filters_large_manifest(admin_config):
    payload = b'{"metadata":{"ok":true},"padding":"' + b"x" * (33 * 1024 * 1024) + b'"}'
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            headers={"Content-Encoding": "gzip"},
            stream=Chunks([gzip.compress(payload)]),
        )
    )
    real_client = httpx.AsyncClient
    client = DbtAdminAPIClient(MockAdminApiConfigProvider(admin_config))
    with patch(
        "httpx.AsyncClient",
        side_effect=lambda **kw: real_client(transport=transport, **kw),
    ):
        result = await client.get_job_run_artifact(
            12345, 100, "manifest.json", jq_filter=".metadata"
        )
    assert result == '[{"ok":true}]'
