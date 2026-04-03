import asyncio
import base64
import hashlib
import logging
import secrets
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode, urlparse

import httpx
import jwt
from pydantic import AnyHttpUrl
from jwt import PyJWKClient
from jwt.exceptions import PyJWTError
from starlette.requests import Request
from starlette.responses import PlainTextResponse, RedirectResponse, Response

from mcp.server.auth.provider import (
    AccessToken,
    AuthorizationCode,
    AuthorizationParams,
    OAuthAuthorizationServerProvider,
    RefreshToken,
    TokenVerifier,
    TokenError,
    construct_redirect_uri,
)
from mcp.server.auth.settings import AuthSettings, ClientRegistrationOptions
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken
from mcp.shared.auth_utils import check_resource_allowed, resource_url_from_server_url

from dbt_mcp.config.config import OidcAuthConfig

logger = logging.getLogger(__name__)

INTROSPECTION_UNSUPPORTED_STATUS_CODES = {404, 405}
BROKER_TOKEN_EXCHANGE_ERROR_DESCRIPTION = "OAuth broker token exchange failed"


@dataclass
class PendingAuthorizationRequest:
    client_id: str
    params: AuthorizationParams
    upstream_code_verifier: str


@dataclass
class StoredAuthorizationCode:
    authorization_code: AuthorizationCode
    oauth_token: OAuthToken


class IntrospectionTokenVerifier(TokenVerifier):
    """Validate bearer tokens with introspection or JWT/JWKS fallback."""

    def __init__(
        self,
        *,
        issuer_url: str,
        introspection_endpoint: str | None,
        resource_server_url: str,
        client_id: str,
        client_secret: str | None,
    ) -> None:
        self.issuer_url = issuer_url.rstrip("/")
        self.introspection_endpoint = introspection_endpoint
        self.resource_server_url = resource_server_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.resource_url = resource_url_from_server_url(resource_server_url)
        self._jwks_client: PyJWKClient | None = None
        self._introspection_disabled = False

    async def verify_token(self, token: str) -> AccessToken | None:
        # Prefer introspection when configured.
        # Fall back to JWT/JWKS validation only when introspection is not configured
        # or explicitly unsupported by the provider (404/405).
        if self._should_use_introspection():
            introspection_result = await self._verify_with_introspection(token)
            if introspection_result is not None:
                return introspection_result
            # Introspection is configured, but this verification attempt failed.
            # Fail closed unless introspection has been explicitly disabled because
            # the provider does not support it.
            if not self._introspection_disabled:
                return None
        return await self._verify_with_jwt(token)

    def _should_use_introspection(self) -> bool:
        return bool(self.introspection_endpoint) and not self._introspection_disabled

    async def _verify_with_introspection(self, token: str) -> AccessToken | None:
        if self._introspection_disabled:
            return None
        if not self.introspection_endpoint:
            return None
        if not self._introspection_endpoint_is_allowed():
            logger.warning(
                "OIDC introspection endpoint must be https:// or localhost over http"
            )
            return None
        timeout = httpx.Timeout(10.0, connect=5.0)
        limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)
        async with httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            verify=True,
        ) as client:
            try:
                request_data: dict[str, str] = {
                    "token": token,
                    "client_id": self.client_id,
                }
                if self.client_secret:
                    request_data["client_secret"] = self.client_secret
                response = await client.post(
                    self.introspection_endpoint,
                    data=request_data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
                if response.status_code in INTROSPECTION_UNSUPPORTED_STATUS_CODES:
                    # Provider does not implement introspection at this endpoint.
                    self._introspection_disabled = True
                    return None
                if response.status_code != 200:
                    return None

                token_data = response.json()
                if not isinstance(token_data, dict):
                    return None
                if not token_data.get("active", False):
                    return None
                if not self._validate_resource(token_data):
                    return None

                return self._build_access_token(token=token, token_data=token_data)
            except Exception as e:
                logger.debug("Token introspection failed: %s", e)
                return None

    async def _verify_with_jwt(self, token: str) -> AccessToken | None:
        token_data = await self._decode_jwt_claims(token)
        if token_data is None:
            return None
        if not self._issuer_matches(token_data.get("iss")):
            return None
        if not self._validate_resource(token_data):
            return None
        return self._build_access_token(token=token, token_data=token_data)

    async def _decode_jwt_claims(self, token: str) -> dict[str, Any] | None:
        try:
            header = jwt.get_unverified_header(token)
            algorithm = header.get("alg")
            if not isinstance(algorithm, str) or algorithm.lower() == "none":
                return None

            signing_key = await asyncio.to_thread(
                self._get_jwks_client().get_signing_key_from_jwt, token
            )
            claims = jwt.decode(
                token,
                signing_key.key,
                algorithms=[algorithm],
                options={"verify_aud": False},
            )
            return claims if isinstance(claims, dict) else None
        except (PyJWTError, ValueError, TypeError) as e:
            logger.debug("JWT verification failed: %s", e)
            return None

    def _get_jwks_client(self) -> PyJWKClient:
        if self._jwks_client is None:
            self._jwks_client = PyJWKClient(f"{self.issuer_url}/.well-known/jwks.json")
        return self._jwks_client

    def _introspection_endpoint_is_allowed(self) -> bool:
        return bool(self.introspection_endpoint) and self.introspection_endpoint.startswith(
            ("https://", "http://localhost", "http://127.0.0.1")
        )

    def _validate_resource(self, token_data: dict[str, Any]) -> bool:
        if not self.resource_server_url or not self.resource_url:
            return False
        audiences = self._parse_audiences(token_data.get("aud"))
        return any(self._is_valid_resource(audience) for audience in audiences)

    def _is_valid_resource(self, resource: str) -> bool:
        return check_resource_allowed(self.resource_url, resource)

    def _parse_audiences(self, audience: Any) -> list[str]:
        if isinstance(audience, str):
            return [audience]
        if isinstance(audience, list):
            return [value for value in audience if isinstance(value, str)]
        return []

    def _issuer_matches(self, issuer: Any) -> bool:
        if not isinstance(issuer, str):
            return False
        return issuer.rstrip("/") == self.issuer_url

    def _parse_scopes(self, scopes: Any) -> list[str]:
        if isinstance(scopes, str):
            return [scope for scope in scopes.split() if scope]
        if isinstance(scopes, list):
            return [scope for scope in scopes if isinstance(scope, str)]
        return []

    def _extract_scopes(self, token_data: dict[str, Any]) -> list[str]:
        scope_values = self._parse_scopes(token_data.get("scope"))
        if scope_values:
            return scope_values
        return self._parse_scopes(token_data.get("scp"))

    def _extract_client_id(self, token_data: dict[str, Any]) -> str:
        for claim_name in ("client_id", "azp", "cid", "sub"):
            claim = token_data.get(claim_name)
            if isinstance(claim, str) and claim:
                return claim
        return "unknown"

    def _extract_resource(self, token_data: dict[str, Any]) -> str | None:
        audiences = self._parse_audiences(token_data.get("aud"))
        return audiences[0] if audiences else None

    def _extract_expiry(self, token_data: dict[str, Any]) -> int | None:
        expires_at = token_data.get("exp")
        return expires_at if isinstance(expires_at, int) else None

    def _build_access_token(
        self, *, token: str, token_data: dict[str, Any]
    ) -> AccessToken:
        return AccessToken(
            token=token,
            client_id=self._extract_client_id(token_data),
            scopes=self._extract_scopes(token_data),
            expires_at=self._extract_expiry(token_data),
            resource=self._extract_resource(token_data),
        )


class OidcAuthorizationCodeBrokerProvider(
    OAuthAuthorizationServerProvider[AuthorizationCode, RefreshToken, AccessToken]
):
    """OAuth AS broker that fronts an upstream OIDC provider with MCP-native DCR."""

    def __init__(
        self,
        *,
        upstream_issuer_url: str,
        upstream_client_id: str,
        upstream_client_secret: str | None,
        callback_url: str,
        default_scopes: list[str],
        fallback_token_verifier: TokenVerifier,
    ) -> None:
        self.upstream_issuer_url = upstream_issuer_url.rstrip("/")
        self.upstream_client_id = upstream_client_id
        self.upstream_client_secret = upstream_client_secret
        self.callback_url = callback_url
        self.callback_path = urlparse(callback_url).path or "/callback"
        self.default_scopes = default_scopes
        self._fallback_token_verifier = fallback_token_verifier

        self._clients: dict[str, OAuthClientInformationFull] = {}
        self._pending_authorizations: dict[str, PendingAuthorizationRequest] = {}
        self._authorization_codes: dict[str, StoredAuthorizationCode] = {}
        self._refresh_tokens: dict[str, RefreshToken] = {}
        self._access_tokens: dict[str, AccessToken] = {}
        self._state_lock = asyncio.Lock()
        self._discovery_lock = asyncio.Lock()
        self._oidc_discovery: dict[str, Any] | None = None

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        return self._clients.get(client_id)

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        if not client_info.client_id:
            raise ValueError("Missing client_id")
        self._clients[client_info.client_id] = client_info

    async def authorize(
        self, client: OAuthClientInformationFull, params: AuthorizationParams
    ) -> str:
        discovery = await self._get_oidc_discovery()
        authorization_endpoint = discovery["authorization_endpoint"]
        broker_state = secrets.token_urlsafe(32)
        upstream_code_verifier = secrets.token_urlsafe(64)
        upstream_code_challenge = self._pkce_s256_challenge(upstream_code_verifier)
        async with self._state_lock:
            self._pending_authorizations[broker_state] = PendingAuthorizationRequest(
                client_id=str(client.client_id),
                params=params,
                upstream_code_verifier=upstream_code_verifier,
            )

        upstream_scopes = self._resolve_upstream_scopes(
            params.scopes,
            discovery.get("scopes_supported"),
        )
        query_params = {
            "response_type": "code",
            "client_id": self.upstream_client_id,
            "redirect_uri": self.callback_url,
            "scope": " ".join(upstream_scopes),
            "state": broker_state,
            "code_challenge": upstream_code_challenge,
            "code_challenge_method": "S256",
        }
        return f"{authorization_endpoint}?{urlencode(query_params)}"

    async def load_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: str
    ) -> AuthorizationCode | None:
        stored = self._authorization_codes.get(authorization_code)
        if not stored:
            return None
        if stored.authorization_code.client_id != client.client_id:
            return None
        return stored.authorization_code

    async def exchange_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: AuthorizationCode
    ) -> OAuthToken:
        stored = self._authorization_codes.pop(authorization_code.code, None)
        if stored is None:
            raise TokenError(
                error="invalid_grant",
                error_description="authorization code does not exist",
            )
        token = stored.oauth_token
        mcp_scopes = stored.authorization_code.scopes or self.default_scopes
        self._access_tokens[token.access_token] = AccessToken(
            token=token.access_token,
            client_id=str(client.client_id),
            scopes=mcp_scopes,
            expires_at=self._compute_expiry(token.expires_in),
            resource=authorization_code.resource,
        )
        if token.refresh_token:
            self._refresh_tokens[token.refresh_token] = RefreshToken(
                token=token.refresh_token,
                client_id=str(client.client_id),
                scopes=mcp_scopes,
                expires_at=None,
            )
        return token

    async def load_refresh_token(
        self, client: OAuthClientInformationFull, refresh_token: str
    ) -> RefreshToken | None:
        token = self._refresh_tokens.get(refresh_token)
        if token is None:
            return None
        if token.client_id != client.client_id:
            return None
        return token

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        discovery = await self._get_oidc_discovery()
        token_endpoint = discovery["token_endpoint"]
        payload: dict[str, str] = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token.token,
            "client_id": self.upstream_client_id,
        }
        if self.upstream_client_secret:
            payload["client_secret"] = self.upstream_client_secret

        timeout = httpx.Timeout(10.0, connect=5.0)
        async with httpx.AsyncClient(timeout=timeout, verify=True) as client_http:
            response = await client_http.post(
                token_endpoint,
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
        if response.status_code != 200:
            raise TokenError(
                error="invalid_grant",
                error_description="upstream refresh token exchange failed",
            )
        token = self._parse_oauth_token(response.json())
        resolved_scopes = scopes or refresh_token.scopes or self.default_scopes
        effective_refresh_token = token.refresh_token or refresh_token.token

        self._access_tokens[token.access_token] = AccessToken(
            token=token.access_token,
            client_id=str(client.client_id),
            scopes=resolved_scopes,
            expires_at=self._compute_expiry(token.expires_in),
            resource=None,
        )
        self._refresh_tokens[effective_refresh_token] = RefreshToken(
            token=effective_refresh_token,
            client_id=str(client.client_id),
            scopes=resolved_scopes,
            expires_at=None,
        )
        if effective_refresh_token != refresh_token.token:
            self._refresh_tokens.pop(refresh_token.token, None)
        if not token.refresh_token:
            token = OAuthToken(
                access_token=token.access_token,
                token_type=token.token_type,
                expires_in=token.expires_in,
                scope=token.scope,
                refresh_token=effective_refresh_token,
            )
        return token

    async def load_access_token(self, token: str) -> AccessToken | None:
        stored_token = self._access_tokens.get(token)
        if stored_token is not None:
            return stored_token
        return await self._fallback_token_verifier.verify_token(token)

    async def revoke_token(self, token: AccessToken | RefreshToken) -> None:
        if isinstance(token, AccessToken):
            self._access_tokens.pop(token.token, None)
            return
        self._refresh_tokens.pop(token.token, None)

    async def handle_callback(self, request: Request) -> Response:
        params = request.query_params
        broker_state = params.get("state")
        if not broker_state:
            return PlainTextResponse("Missing state parameter", status_code=400)

        async with self._state_lock:
            pending = self._pending_authorizations.pop(broker_state, None)

        if pending is None:
            return PlainTextResponse("Invalid or expired state parameter", status_code=400)

        if params.get("error"):
            redirect_url = construct_redirect_uri(
                str(pending.params.redirect_uri),
                error=params.get("error"),
                error_description=params.get("error_description"),
                state=pending.params.state,
            )
            return RedirectResponse(url=redirect_url, status_code=302)

        upstream_code = params.get("code")
        if not upstream_code:
            return PlainTextResponse("Missing authorization code", status_code=400)

        try:
            token = await self._exchange_upstream_authorization_code(
                upstream_code,
                pending.upstream_code_verifier,
            )
        except Exception:
            logger.exception("Broker callback token exchange failed")
            redirect_url = construct_redirect_uri(
                str(pending.params.redirect_uri),
                error="server_error",
                error_description=BROKER_TOKEN_EXCHANGE_ERROR_DESCRIPTION,
                state=pending.params.state,
            )
            return RedirectResponse(url=redirect_url, status_code=302)

        mcp_auth_code = secrets.token_urlsafe(32)
        self._authorization_codes[mcp_auth_code] = StoredAuthorizationCode(
            authorization_code=AuthorizationCode(
                code=mcp_auth_code,
                scopes=pending.params.scopes or self.default_scopes,
                expires_at=time.time() + 300,
                client_id=pending.client_id,
                code_challenge=pending.params.code_challenge,
                redirect_uri=pending.params.redirect_uri,
                redirect_uri_provided_explicitly=(
                    pending.params.redirect_uri_provided_explicitly
                ),
                resource=pending.params.resource,
            ),
            oauth_token=token,
        )

        redirect_url = construct_redirect_uri(
            str(pending.params.redirect_uri),
            code=mcp_auth_code,
            state=pending.params.state,
        )
        return RedirectResponse(url=redirect_url, status_code=302)

    async def _exchange_upstream_authorization_code(
        self,
        upstream_code: str,
        code_verifier: str,
    ) -> OAuthToken:
        discovery = await self._get_oidc_discovery()
        token_endpoint = discovery["token_endpoint"]
        payload: dict[str, str] = {
            "grant_type": "authorization_code",
            "code": upstream_code,
            "client_id": self.upstream_client_id,
            "redirect_uri": self.callback_url,
            "code_verifier": code_verifier,
        }
        if self.upstream_client_secret:
            payload["client_secret"] = self.upstream_client_secret

        timeout = httpx.Timeout(10.0, connect=5.0)
        async with httpx.AsyncClient(timeout=timeout, verify=True) as client:
            response = await client.post(
                token_endpoint,
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
        response.raise_for_status()
        return self._parse_oauth_token(response.json())

    async def _get_oidc_discovery(self) -> dict[str, Any]:
        if self._oidc_discovery is not None:
            return self._oidc_discovery

        async with self._discovery_lock:
            if self._oidc_discovery is not None:
                return self._oidc_discovery

            discovery_url = (
                f"{self.upstream_issuer_url}/.well-known/openid-configuration"
            )
            timeout = httpx.Timeout(10.0, connect=5.0)
            async with httpx.AsyncClient(timeout=timeout, verify=True) as client:
                response = await client.get(discovery_url)
            response.raise_for_status()
            payload = response.json()

            authorization_endpoint = payload.get("authorization_endpoint")
            token_endpoint = payload.get("token_endpoint")
            if not isinstance(authorization_endpoint, str) or not isinstance(
                token_endpoint, str
            ):
                raise ValueError("OIDC discovery metadata missing OAuth endpoints")

            scopes_supported_raw = payload.get("scopes_supported")
            scopes_supported: list[str] = (
                [scope for scope in scopes_supported_raw if isinstance(scope, str)]
                if isinstance(scopes_supported_raw, list)
                else []
            )

            self._oidc_discovery = {
                "authorization_endpoint": authorization_endpoint,
                "token_endpoint": token_endpoint,
                "scopes_supported": scopes_supported,
            }
            return self._oidc_discovery

    def _resolve_upstream_scopes(
        self,
        requested_scopes: list[str] | None,
        supported_scopes: Any,
    ) -> list[str]:
        requested = requested_scopes or []
        supported = (
            [scope for scope in supported_scopes if isinstance(scope, str)]
            if isinstance(supported_scopes, list)
            else []
        )
        if supported:
            requested_supported = [scope for scope in requested if scope in supported]
            if requested_supported:
                return requested_supported

            default_oidc_scopes = [
                scope for scope in ["openid", "profile", "email"] if scope in supported
            ]
            if default_oidc_scopes:
                return default_oidc_scopes
            return [supported[0]]

        if requested:
            return requested
        return ["openid", "profile", "email"]

    def _pkce_s256_challenge(self, verifier: str) -> str:
        digest = hashlib.sha256(verifier.encode("ascii")).digest()
        return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")

    def _parse_oauth_token(self, token_payload: Any) -> OAuthToken:
        if not isinstance(token_payload, dict):
            raise ValueError("Invalid token payload")
        return OAuthToken(
            access_token=str(token_payload["access_token"]),
            token_type="Bearer",
            expires_in=(
                int(token_payload["expires_in"])
                if token_payload.get("expires_in") is not None
                else None
            ),
            scope=(
                str(token_payload["scope"])
                if token_payload.get("scope") is not None
                else None
            ),
            refresh_token=(
                str(token_payload["refresh_token"])
                if token_payload.get("refresh_token") is not None
                else None
            ),
        )

    def _compute_expiry(self, expires_in: int | None) -> int | None:
        if expires_in is None:
            return None
        return int(time.time()) + expires_in


def _default_callback_url(resource_server_url: str) -> str:
    parsed = urlparse(resource_server_url)
    return f"{parsed.scheme}://{parsed.netloc}/callback"


def _default_auth_server_issuer_url(resource_server_url: str) -> str:
    parsed = urlparse(resource_server_url)
    return f"{parsed.scheme}://{parsed.netloc}"


def create_oidc_auth_settings(
    auth_config: OidcAuthConfig,
) -> tuple[AuthSettings, TokenVerifier | None, OidcAuthorizationCodeBrokerProvider | None]:
    fallback_token_verifier = IntrospectionTokenVerifier(
        issuer_url=auth_config.token_issuer_url or auth_config.issuer_url,
        introspection_endpoint=auth_config.introspection_endpoint,
        resource_server_url=auth_config.resource_server_url,
        client_id=auth_config.client_id,
        client_secret=auth_config.client_secret,
    )
    native_auth_provider: OidcAuthorizationCodeBrokerProvider | None = None
    if auth_config.native_auth_enabled:
        issuer_url = auth_config.auth_server_issuer_url or _default_auth_server_issuer_url(
            auth_config.resource_server_url
        )
    else:
        issuer_url = auth_config.auth_server_issuer_url or auth_config.issuer_url

    client_registration_options = None
    if auth_config.native_auth_enabled:
        client_registration_options = ClientRegistrationOptions(
            enabled=True,
            valid_scopes=auth_config.required_scopes,
            default_scopes=auth_config.required_scopes,
        )
        native_auth_provider = OidcAuthorizationCodeBrokerProvider(
            upstream_issuer_url=auth_config.issuer_url,
            upstream_client_id=auth_config.client_id,
            upstream_client_secret=auth_config.client_secret,
            callback_url=auth_config.callback_url
            or _default_callback_url(auth_config.resource_server_url),
            default_scopes=auth_config.required_scopes,
            fallback_token_verifier=fallback_token_verifier,
        )

    auth_settings = AuthSettings(
        issuer_url=AnyHttpUrl(issuer_url),
        resource_server_url=AnyHttpUrl(auth_config.resource_server_url),
        required_scopes=auth_config.required_scopes,
        client_registration_options=client_registration_options,
    )
    if auth_config.native_auth_enabled:
        return auth_settings, None, native_auth_provider
    return auth_settings, fallback_token_verifier, native_auth_provider
