import os
from urllib.parse import parse_qs, urlparse
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mcp.server.auth.provider import AuthorizationCode, AuthorizationParams
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken

from dbt_mcp.config.config import load_config
from dbt_mcp.config.config import OidcAuthConfig
from dbt_mcp.mcp.server import create_dbt_mcp
from dbt_mcp.mcp.oidc_auth import (
    BROKER_TOKEN_EXCHANGE_ERROR_DESCRIPTION,
    IntrospectionTokenVerifier,
    OidcAuthorizationCodeBrokerProvider,
    PendingAuthorizationRequest,
    StoredAuthorizationCode,
    create_oidc_auth_settings,
)


# Introspection/JWT token verification behavior
@pytest.mark.asyncio
async def test_verify_token_success():
    verifier = IntrospectionTokenVerifier(
        issuer_url="https://auth.example.com",
        introspection_endpoint="https://auth.example.com/introspect",
        resource_server_url="https://mcp.example.com",
        client_id="client-id",
        client_secret="secret",
    )

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "active": True,
        "client_id": "test-client",
        "scope": "mcp:tools mcp:resources",
        "exp": 1234,
        "aud": "https://mcp.example.com",
    }

    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    mock_async_client.__aexit__.return_value = False

    with patch(
        "dbt_mcp.mcp.oidc_auth.httpx.AsyncClient", return_value=mock_async_client
    ):
        access_token = await verifier.verify_token("token-value")

    assert access_token is not None
    assert access_token.client_id == "test-client"
    assert access_token.scopes == ["mcp:tools", "mcp:resources"]
    assert access_token.expires_at == 1234


@pytest.mark.asyncio
async def test_verify_token_returns_none_for_invalid_audience():
    verifier = IntrospectionTokenVerifier(
        issuer_url="https://auth.example.com",
        introspection_endpoint="https://auth.example.com/introspect",
        resource_server_url="https://mcp.example.com",
        client_id="client-id",
        client_secret="secret",
    )

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "active": True,
        "client_id": "test-client",
        "scope": "mcp:tools",
        "exp": 1234,
        "aud": "https://other-service.example.com",
    }

    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    mock_async_client.__aexit__.return_value = False

    with patch(
        "dbt_mcp.mcp.oidc_auth.httpx.AsyncClient", return_value=mock_async_client
    ):
        access_token = await verifier.verify_token("token-value")

    assert access_token is None


@pytest.mark.asyncio
async def test_verify_token_rejects_insecure_non_localhost_introspection_endpoint():
    verifier = IntrospectionTokenVerifier(
        issuer_url="https://auth.example.com",
        introspection_endpoint="http://auth.example.com/introspect",
        resource_server_url="https://mcp.example.com",
        client_id="client-id",
        client_secret="secret",
    )

    access_token = await verifier.verify_token("token-value")
    assert access_token is None


@pytest.mark.asyncio
async def test_verify_token_uses_jwt_fallback_when_introspection_unavailable():
    verifier = IntrospectionTokenVerifier(
        issuer_url="https://auth.example.com",
        introspection_endpoint="https://auth.example.com/oauth2/introspect",
        resource_server_url="https://mcp.example.com",
        client_id="oidc-client-id",
        client_secret="secret",
    )

    mock_response = MagicMock()
    mock_response.status_code = 404

    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    mock_async_client.__aexit__.return_value = False

    with (
        patch(
            "dbt_mcp.mcp.oidc_auth.httpx.AsyncClient", return_value=mock_async_client
        ),
        patch.object(
            verifier,
            "_decode_jwt_claims",
            new=AsyncMock(
                return_value={
                    "iss": "https://auth.example.com/",
                    "aud": "https://mcp.example.com",
                    "scope": "openid profile email",
                    "exp": 1234,
                    "azp": "oidc-client-id",
                }
            ),
        ),
    ):
        access_token = await verifier.verify_token("token-value")

    assert access_token is not None
    assert access_token.client_id == "oidc-client-id"
    assert access_token.scopes == ["openid", "profile", "email"]
    assert access_token.expires_at == 1234


@pytest.mark.asyncio
async def test_verify_token_rejects_client_id_in_aud_without_resource_match():
    verifier = IntrospectionTokenVerifier(
        issuer_url="https://auth.example.com",
        introspection_endpoint=None,
        resource_server_url="https://mcp.example.com",
        client_id="oidc-client-id",
        client_secret=None,
    )

    with patch.object(
        verifier,
        "_decode_jwt_claims",
        new=AsyncMock(
            return_value={
                "iss": "https://auth.example.com/",
                "aud": "oidc-client-id",
                "scope": "openid profile email",
                "exp": 1234,
            }
        ),
    ):
        access_token = await verifier.verify_token("token-value")

    assert access_token is None


@pytest.mark.asyncio
async def test_verify_token_disables_introspection_after_404():
    verifier = IntrospectionTokenVerifier(
        issuer_url="https://auth.example.com",
        introspection_endpoint="https://auth.example.com/oauth2/introspect",
        resource_server_url="https://mcp.example.com",
        client_id="oidc-client-id",
        client_secret="secret",
    )

    mock_response = MagicMock()
    mock_response.status_code = 404

    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    mock_async_client.__aexit__.return_value = False

    jwt_claims = {
        "iss": "https://auth.example.com/",
        "aud": "https://mcp.example.com",
        "scope": "openid profile email",
        "exp": 1234,
        "azp": "oidc-client-id",
    }
    with (
        patch(
            "dbt_mcp.mcp.oidc_auth.httpx.AsyncClient", return_value=mock_async_client
        ),
        patch.object(
            verifier,
            "_decode_jwt_claims",
            new=AsyncMock(return_value=jwt_claims),
        ),
    ):
        first_access_token = await verifier.verify_token("token-one")
        second_access_token = await verifier.verify_token("token-two")

    assert first_access_token is not None
    assert second_access_token is not None
    assert mock_client.post.await_count == 1


@pytest.mark.asyncio
async def test_verify_token_does_not_fallback_to_jwt_on_introspection_500():
    verifier = IntrospectionTokenVerifier(
        issuer_url="https://auth.example.com",
        introspection_endpoint="https://auth.example.com/oauth2/introspect",
        resource_server_url="https://mcp.example.com",
        client_id="oidc-client-id",
        client_secret="secret",
    )

    mock_response = MagicMock()
    mock_response.status_code = 500

    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    mock_async_client.__aexit__.return_value = False

    with (
        patch(
            "dbt_mcp.mcp.oidc_auth.httpx.AsyncClient", return_value=mock_async_client
        ),
        patch.object(
            verifier,
            "_decode_jwt_claims",
            new=AsyncMock(
                return_value={
                    "iss": "https://auth.example.com/",
                    "aud": "oidc-client-id",
                    "scope": "openid profile email",
                    "exp": 1234,
                    "azp": "oidc-client-id",
                }
            ),
        ) as decode_jwt_claims,
    ):
        access_token = await verifier.verify_token("token-value")

    assert access_token is None
    decode_jwt_claims.assert_not_awaited()


@pytest.mark.asyncio
async def test_verify_token_does_not_fallback_to_jwt_on_introspection_exception():
    verifier = IntrospectionTokenVerifier(
        issuer_url="https://auth.example.com",
        introspection_endpoint="https://auth.example.com/oauth2/introspect",
        resource_server_url="https://mcp.example.com",
        client_id="oidc-client-id",
        client_secret="secret",
    )

    mock_client = AsyncMock()
    mock_client.post.side_effect = RuntimeError("introspection unavailable")
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    mock_async_client.__aexit__.return_value = False

    with (
        patch(
            "dbt_mcp.mcp.oidc_auth.httpx.AsyncClient", return_value=mock_async_client
        ),
        patch.object(
            verifier,
            "_decode_jwt_claims",
            new=AsyncMock(
                return_value={
                    "iss": "https://auth.example.com/",
                    "aud": "oidc-client-id",
                    "scope": "openid profile email",
                    "exp": 1234,
                    "azp": "oidc-client-id",
                }
            ),
        ) as decode_jwt_claims,
    ):
        access_token = await verifier.verify_token("token-value")

    assert access_token is None
    decode_jwt_claims.assert_not_awaited()


# Auth settings construction
def test_create_oidc_auth_settings():
    auth_settings, token_verifier, auth_server_provider = create_oidc_auth_settings(
        OidcAuthConfig(
            issuer_url="https://auth.example.com/realms/master",
            resource_server_url="https://mcp.example.com",
            introspection_endpoint=(
                "https://auth.example.com/realms/master/"
                "protocol/openid-connect/token/introspect"
            ),
            client_id="mcp-server",
            client_secret="secret",
            required_scopes=["mcp:tools"],
        )
    )

    assert str(auth_settings.issuer_url).startswith(
        "https://auth.example.com/realms/master"
    )
    assert str(auth_settings.resource_server_url).startswith("https://mcp.example.com")
    assert auth_settings.required_scopes == ["mcp:tools"]
    assert isinstance(token_verifier, IntrospectionTokenVerifier)
    assert auth_server_provider is None


def test_create_oidc_auth_settings_native_auth_enabled():
    auth_settings, token_verifier, auth_server_provider = create_oidc_auth_settings(
        OidcAuthConfig(
            issuer_url="https://auth.example.com",
            resource_server_url="http://127.0.0.1:8788/mcp",
            introspection_endpoint=None,
            client_id="mcp-server",
            client_secret="secret",
            required_scopes=["openid", "profile", "email"],
            native_auth_enabled=True,
        )
    )

    assert str(auth_settings.issuer_url).startswith("http://127.0.0.1:8788")
    assert auth_settings.client_registration_options is not None
    assert auth_settings.client_registration_options.enabled is True
    assert token_verifier is None
    assert auth_server_provider is not None
    assert auth_server_provider.callback_path == "/callback"


# Native broker route exposure and OAuth flow behavior
@pytest.mark.asyncio
async def test_native_auth_provider_exposes_oauth_routes():
    env_vars = {
        "DBT_MCP_OIDC_ENABLED": "true",
        "DBT_MCP_OIDC_ISSUER_URL": "https://auth.example.com",
        "DBT_MCP_OIDC_RESOURCE_SERVER_URL": "http://127.0.0.1:8788/mcp",
        "DBT_MCP_OIDC_CLIENT_ID": "oidc-client-id",
        "DBT_MCP_OIDC_CLIENT_SECRET": "oidc-client-secret",
        "DBT_MCP_OIDC_REQUIRED_SCOPES": "openid,profile,email",
        "DBT_MCP_OIDC_NATIVE_AUTH_ENABLED": "true",
        "DBT_MCP_OIDC_AUTH_SERVER_ISSUER_URL": "http://127.0.0.1:8788",
        "DBT_MCP_OIDC_CALLBACK_URL": "http://127.0.0.1:8788/callback",
    }
    with patch.dict(os.environ, env_vars, clear=True):
        config = load_config(enable_proxied_tools=False)
        dbt_mcp = await create_dbt_mcp(config)
        app = dbt_mcp.streamable_http_app()

    route_paths = {route.path for route in app.routes}
    assert "/register" in route_paths
    assert "/authorize" in route_paths
    assert "/token" in route_paths
    assert "/callback" in route_paths
    assert "/.well-known/oauth-authorization-server" in route_paths
    assert "/.well-known/oauth-protected-resource/mcp" in route_paths


@pytest.mark.asyncio
async def test_native_broker_uses_oidc_scopes_upstream():
    fallback_verifier = AsyncMock()
    fallback_verifier.verify_token = AsyncMock(return_value=None)
    provider = OidcAuthorizationCodeBrokerProvider(
        upstream_issuer_url="https://auth.example.com",
        upstream_client_id="oidc-client-id",
        upstream_client_secret="oidc-client-secret",
        callback_url="http://127.0.0.1:8788/callback",
        default_scopes=["mcp:tools"],
        fallback_token_verifier=fallback_verifier,
    )

    client_code_challenge = "client-code-challenge"
    with patch.object(
        provider,
        "_get_oidc_discovery",
        new=AsyncMock(
            return_value={
                "authorization_endpoint": "https://auth.example.com/oauth2/auth",
                "token_endpoint": "https://auth.example.com/oauth2/token",
                "scopes_supported": [
                    "offline_access",
                    "openid",
                    "email",
                    "profile",
                ],
            }
        ),
    ):
        authorize_url = await provider.authorize(
            OAuthClientInformationFull(
                client_id="codex-client",
                redirect_uris=["http://127.0.0.1:56491/callback"],
            ),
            AuthorizationParams(
                state="client-state",
                scopes=["mcp:tools"],
                code_challenge=client_code_challenge,
                redirect_uri="http://127.0.0.1:56491/callback",
                redirect_uri_provided_explicitly=True,
                resource="http://127.0.0.1:8788/mcp",
            ),
        )

    query = parse_qs(urlparse(authorize_url).query)
    assert query["scope"] == ["openid profile email"]
    assert query["code_challenge"] != [client_code_challenge]


@pytest.mark.asyncio
async def test_native_broker_exchange_keeps_mcp_scopes():
    fallback_verifier = AsyncMock()
    fallback_verifier.verify_token = AsyncMock(return_value=None)
    provider = OidcAuthorizationCodeBrokerProvider(
        upstream_issuer_url="https://auth.example.com",
        upstream_client_id="oidc-client-id",
        upstream_client_secret="oidc-client-secret",
        callback_url="http://127.0.0.1:8788/callback",
        default_scopes=["mcp:tools"],
        fallback_token_verifier=fallback_verifier,
    )
    provider._authorization_codes["mcp-auth-code"] = StoredAuthorizationCode(
        authorization_code=AuthorizationCode(
            code="mcp-auth-code",
            client_id="codex-client",
            scopes=["mcp:tools"],
            expires_at=9999999999,
            code_challenge="challenge",
            redirect_uri="http://127.0.0.1:56491/callback",
            redirect_uri_provided_explicitly=True,
            resource="http://127.0.0.1:8788/mcp",
        ),
        oauth_token=OAuthToken(
            access_token="upstream-access-token",
            token_type="Bearer",
            scope="openid profile email",
            expires_in=3600,
            refresh_token="upstream-refresh-token",
        ),
    )

    await provider.exchange_authorization_code(
        OAuthClientInformationFull(
            client_id="codex-client",
            redirect_uris=["http://127.0.0.1:56491/callback"],
        ),
        provider._authorization_codes["mcp-auth-code"].authorization_code,
    )

    assert provider._access_tokens["upstream-access-token"].scopes == ["mcp:tools"]


@pytest.mark.asyncio
async def test_native_broker_token_exchange_sends_upstream_code_verifier():
    fallback_verifier = AsyncMock()
    fallback_verifier.verify_token = AsyncMock(return_value=None)
    provider = OidcAuthorizationCodeBrokerProvider(
        upstream_issuer_url="https://auth.example.com",
        upstream_client_id="oidc-client-id",
        upstream_client_secret="oidc-client-secret",
        callback_url="http://localhost:8788/callback",
        default_scopes=["mcp:tools"],
        fallback_token_verifier=fallback_verifier,
    )
    with patch.object(
        provider,
        "_get_oidc_discovery",
        new=AsyncMock(
            return_value={
                "authorization_endpoint": "https://auth.example.com/oauth2/auth",
                "token_endpoint": "https://auth.example.com/oauth2/token",
                "scopes_supported": ["openid", "profile", "email"],
            }
        ),
    ):
        authorize_url = await provider.authorize(
            OAuthClientInformationFull(
                client_id="codex-client",
                redirect_uris=["http://127.0.0.1:56491/callback"],
            ),
            AuthorizationParams(
                state="client-state",
                scopes=["mcp:tools"],
                code_challenge="client-code-challenge",
                redirect_uri="http://127.0.0.1:56491/callback",
                redirect_uri_provided_explicitly=True,
                resource="http://127.0.0.1:8788/mcp",
            ),
        )

    broker_state = parse_qs(urlparse(authorize_url).query)["state"][0]
    pending = provider._pending_authorizations[broker_state]
    provider._oidc_discovery = {
        "authorization_endpoint": "https://auth.example.com/oauth2/auth",
        "token_endpoint": "https://auth.example.com/oauth2/token",
        "scopes_supported": ["openid", "profile", "email"],
    }

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock(return_value=None)
    mock_response.json.return_value = {
        "access_token": "upstream-access-token",
        "token_type": "Bearer",
        "scope": "openid profile email",
        "expires_in": 3600,
    }
    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    mock_async_client.__aexit__.return_value = False

    with patch("dbt_mcp.mcp.oidc_auth.httpx.AsyncClient", return_value=mock_async_client):
        await provider._exchange_upstream_authorization_code(
            "upstream-code",
            pending.upstream_code_verifier,
        )

    call_kwargs = mock_client.post.await_args.kwargs
    assert call_kwargs["data"]["code_verifier"] == pending.upstream_code_verifier


# Native broker callback/error handling behavior
@pytest.mark.asyncio
async def test_native_broker_callback_error_description_is_sanitized():
    fallback_verifier = AsyncMock()
    fallback_verifier.verify_token = AsyncMock(return_value=None)
    provider = OidcAuthorizationCodeBrokerProvider(
        upstream_issuer_url="https://auth.example.com",
        upstream_client_id="oidc-client-id",
        upstream_client_secret="oidc-client-secret",
        callback_url="http://127.0.0.1:8788/callback",
        default_scopes=["mcp:tools"],
        fallback_token_verifier=fallback_verifier,
    )

    provider._pending_authorizations["broker-state"] = PendingAuthorizationRequest(
        client_id="codex-client",
        params=AuthorizationParams(
            state="client-state",
            scopes=["mcp:tools"],
            code_challenge="client-code-challenge",
            redirect_uri="http://127.0.0.1:56491/callback",
            redirect_uri_provided_explicitly=True,
            resource="http://127.0.0.1:8788/mcp",
        ),
        upstream_code_verifier="upstream-code-verifier",
    )
    request = MagicMock()
    request.query_params = {"state": "broker-state", "code": "upstream-code"}

    with patch.object(
        provider,
        "_exchange_upstream_authorization_code",
        new=AsyncMock(side_effect=RuntimeError("sensitive upstream detail")),
    ):
        response = await provider.handle_callback(request)

    assert response.status_code == 302
    location = response.headers["location"]
    query = parse_qs(urlparse(location).query)
    assert query["error"] == ["server_error"]
    assert query["error_description"] == [BROKER_TOKEN_EXCHANGE_ERROR_DESCRIPTION]
    assert "sensitive upstream detail" not in location
