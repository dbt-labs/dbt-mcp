import json
from contextlib import asynccontextmanager
from dataclasses import replace
from unittest.mock import patch

import httpx
import pytest

from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.dbt_admin.tools import AdminToolContext, get_job_run_details
from dbt_mcp.config.config_providers import AdminApiConfig
from dbt_mcp.discovery.client import execute_query
from dbt_mcp.product_docs.client import ProductDocsClient
from dbt_mcp.semantic_layer.gql.gql_request import submit_request
from dbt_mcp.errors import ResponseLimitError
from dbt_mcp.resource_limits import (
    ArtifactConfig,
    HttpConfig,
    ResponseLimits,
    ResponseSize,
)
from tests.unit.dbt_admin.test_client import (
    MockAdminApiConfigProvider,
    MockHeadersProvider,
)
from tests.mocks.config import mock_config


@pytest.mark.parametrize(
    "service", ["admin", "artifact", "discovery", "semantic_layer", "docs"]
)
async def test_injected_admission_and_byte_budget_cover_acquisition(service):
    events = []

    @asynccontextmanager
    async def admission():
        events.append("enter")
        try:
            yield
        finally:
            events.append("exit")

    def respond(request):
        events.append("request")
        assert events[0] == "enter"
        return httpx.Response(200, content=b'{"data":{"id":1}}')

    budgets = ResponseLimits(8, 8)
    config = replace(
        AdminApiConfig(
            url="https://cloud.getdbt.com",
            account_id=12345,
            headers_provider=MockHeadersProvider({}),
        ),
        http_config=HttpConfig(response_limits=budgets, admission=admission),
        artifact_config=ArtifactConfig(response_limits=budgets, admission=admission),
    )
    client = DbtAdminAPIClient(MockAdminApiConfigProvider(config))
    client_class = httpx.AsyncClient
    with patch(
        "httpx.AsyncClient",
        side_effect=lambda **kwargs: client_class(
            transport=httpx.MockTransport(respond), **kwargs
        ),
    ):
        with pytest.raises(ResponseLimitError):
            if service == "artifact":
                await client.get_job_run_artifact(
                    12345, 100, "manifest.json", jq_filter="."
                )
            elif service == "admin":
                await client.get_account(12345)
            elif service == "discovery":
                discovery = await mock_config.discovery_config_provider.get_config()
                await execute_query(
                    "query",
                    {},
                    config=replace(discovery, http_config=config.http_config),
                )
            elif service == "semantic_layer":
                semantic_layer = (
                    await mock_config.semantic_layer_config_provider.get_config()
                )
                await submit_request(
                    replace(semantic_layer, http_config=config.http_config), {}
                )
            else:
                await ProductDocsClient(http_config=config.http_config).get_page(
                    "https://docs.getdbt.com/docs"
                )
    assert events == ["enter", "request", "exit"]


@pytest.mark.parametrize(
    "budgets", [ResponseLimits(wire_bytes=512), ResponseLimits(decoded_bytes=512)]
)
async def test_run_details_tool_omits_steps_whose_logs_exceed_the_budget(budgets):
    def respond(request):
        if "include_related" in request.url.params:
            run = {"id": 100, "run_steps": [{"index": 1, "logs": "x" * 1024}]}
        else:
            run = {"id": 100, "status": 20, "run_steps": []}
        body = json.dumps({"data": run}).encode()
        return httpx.Response(
            200,
            headers={"content-length": str(len(body))},
            stream=httpx.ByteStream(body),
        )

    config = AdminApiConfig(
        url="https://cloud.getdbt.com",
        account_id=12345,
        headers_provider=MockHeadersProvider({}),
        http_config=HttpConfig(response_limits=budgets),
    )
    context = AdminToolContext(MockAdminApiConfigProvider(config))
    client_class = httpx.AsyncClient
    with patch(
        "httpx.AsyncClient",
        side_effect=lambda **kwargs: client_class(
            transport=httpx.MockTransport(respond), **kwargs
        ),
    ):
        result = await get_job_run_details.fn(context, run_id=100)

    assert result["id"] == 100
    assert result["status"] == 20
    assert "run_steps" not in result
    assert "size limit" in result["run_steps_omitted"]


@pytest.mark.parametrize(
    "service",
    [
        "admin",
        "admin.run_details",
        "artifact.manifest",
        "artifact.other",
        "discovery",
        "semantic_layer",
        "product_docs.index",
        "product_docs.page",
    ],
)
async def test_response_size_observer_is_injected_into_each_client(service):
    observations = []
    payload = b'{"data":{"id":1}}'
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, stream=httpx.ByteStream(payload))
    )
    config = AdminApiConfig(
        url="https://cloud.getdbt.com",
        account_id=12345,
        headers_provider=MockHeadersProvider({}),
        http_config=HttpConfig(observer=observations.append),
        artifact_config=ArtifactConfig(observer=observations.append),
    )
    client = DbtAdminAPIClient(MockAdminApiConfigProvider(config))
    client_class = httpx.AsyncClient
    with patch(
        "httpx.AsyncClient",
        side_effect=lambda **kwargs: client_class(transport=transport, **kwargs),
    ):
        if service.startswith("artifact."):
            await client.get_job_run_artifact(
                12345,
                100,
                "manifest.json" if service == "artifact.manifest" else "custom.json",
                jq_filter=".data",
            )
        elif service == "admin":
            await client.get_account(12345)
        elif service == "admin.run_details":
            await client.get_job_run_details(12345, 100)
        elif service == "discovery":
            discovery = await mock_config.discovery_config_provider.get_config()
            await execute_query(
                "query", {}, config=replace(discovery, http_config=config.http_config)
            )
        elif service == "semantic_layer":
            semantic_layer = (
                await mock_config.semantic_layer_config_provider.get_config()
            )
            await submit_request(
                replace(semantic_layer, http_config=config.http_config), {}
            )
        else:
            docs = ProductDocsClient(http_config=config.http_config)
            if service == "product_docs.index":
                await docs.get_index()
            else:
                await docs.get_page("https://docs.getdbt.com/docs")
    assert observations == [ResponseSize(service, len(payload), len(payload), True)]
