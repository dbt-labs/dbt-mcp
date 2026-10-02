import gzip
import json
import zlib
from unittest.mock import patch

import httpx
import pytest

from dbt_mcp.discovery.client import execute_query
from dbt_mcp.errors import InvalidParameterError
from tests.unit.dbt_admin.test_artifact_limits import Chunks


@pytest.mark.parametrize("encoding", ["identity", "gzip", "deflate"])
async def test_metadata_decodes_bounded_responses(unit_discovery_config, encoding):
    payload = json.dumps({"data": {"description": "café"}}).encode()
    if encoding == "gzip":
        payload = gzip.compress(payload)
    if encoding == "deflate":
        payload = zlib.compress(payload)
    stream = Chunks([payload[:5], payload[5:]])
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, headers={"Content-Encoding": encoding}, stream=stream
        )
    )
    client_class = httpx.AsyncClient
    with patch(
        "httpx.AsyncClient",
        side_effect=lambda **kwargs: client_class(transport=transport, **kwargs),
    ):
        result = await execute_query("query", {}, config=unit_discovery_config)
    assert result == {"data": {"description": "café"}}
    assert stream.closed


async def test_metadata_compression_bomb_stops_before_json_parsing(
    unit_discovery_config,
):
    stream = Chunks([gzip.compress(b"x" * (3 * 1024 * 1024)), b"unused"])
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            headers={"Content-Encoding": "gzip", "Content-Length": "1"},
            stream=stream,
        )
    )
    client_class = httpx.AsyncClient
    with patch(
        "httpx.AsyncClient",
        side_effect=lambda **kwargs: client_class(transport=transport, **kwargs),
    ):
        with pytest.raises(InvalidParameterError, match="decoded size limit"):
            await execute_query("query", {}, config=unit_discovery_config)
    assert stream.read_count == 1
    assert stream.closed
