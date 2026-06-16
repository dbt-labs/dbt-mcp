from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dbt_mcp.dbt_admin.onboarding.account_client import AccountClient
from dbt_mcp.errors import InvalidParameterError


def _creds(actual_host, actual_host_prefix=None, base_host=None):
    return SimpleNamespace(
        settings=SimpleNamespace(
            actual_host=actual_host,
            actual_host_prefix=actual_host_prefix,
            base_host=base_host or actual_host,
        )
    )


def _mock_httpx_client(mock_response):
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    return mock_client


async def test_create_posts_unauthenticated_to_accounts_url():
    client = AccountClient(_creds("cloud.getdbt.com"))
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"account_id": 7, "owner_token": "tok"}}
    mock_response.raise_for_status.return_value = None
    mock_client = _mock_httpx_client(mock_response)

    with patch("httpx.AsyncClient", return_value=mock_client):
        data = await client.create(name="acme", owner_email="o@acme.example")

    assert data == {"account_id": 7, "owner_token": "tok"}
    args, kwargs = mock_client.post.call_args
    assert args[0] == "https://cloud.getdbt.com/api/v3/accounts/"
    assert kwargs["json"] == {"name": "acme", "owner_email": "o@acme.example"}
    # Public endpoint — no Authorization header is sent.
    assert "Authorization" not in kwargs["headers"]


async def test_create_includes_created_via_when_provided():
    client = AccountClient(_creds("cloud.getdbt.com"))
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"account_id": 7, "owner_token": "tok"}}
    mock_response.raise_for_status.return_value = None
    mock_client = _mock_httpx_client(mock_response)

    with patch("httpx.AsyncClient", return_value=mock_client):
        await client.create(
            name="acme", owner_email="o@acme.example", created_via="onboarding_api"
        )

    assert mock_client.post.call_args.kwargs["json"]["created_via"] == "onboarding_api"


async def test_create_uses_host_prefix():
    client = AccountClient(_creds("cloud.getdbt.com", actual_host_prefix="ab123"))
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"account_id": 1, "owner_token": "t"}}
    mock_response.raise_for_status.return_value = None
    mock_client = _mock_httpx_client(mock_response)

    with patch("httpx.AsyncClient", return_value=mock_client):
        await client.create(name="a", owner_email="o@a.example")

    assert (
        mock_client.post.call_args.args[0]
        == "https://ab123.cloud.getdbt.com/api/v3/accounts/"
    )


async def test_create_without_host_raises():
    client = AccountClient(_creds(None))
    with pytest.raises(InvalidParameterError):
        await client.create(name="a", owner_email="o@a.example")
