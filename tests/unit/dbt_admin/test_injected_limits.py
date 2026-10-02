from contextlib import asynccontextmanager
from dataclasses import replace
from unittest.mock import patch

import httpx
import pytest

from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.config.config_providers import AdminApiConfig
from dbt_mcp.discovery.client import execute_query
from dbt_mcp.product_docs.client import ProductDocsClient
from dbt_mcp.semantic_layer.gql.gql_request import submit_request
from dbt_mcp.errors import ResponseLimitError
from dbt_mcp.resource_limits import ArtifactConfig, HttpConfig, ResponseLimits
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
