from functools import lru_cache
from typing import Any

import jwt
from jwt import PyJWKClient
from pydantic import BaseModel


@lru_cache(maxsize=1)
def _get_jwks_client(jwks_url: str) -> PyJWKClient:
    return PyJWKClient(jwks_url)


def _clear_jwks_cache() -> None:
    _get_jwks_client.cache_clear()


class AccessTokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    expires_in: int
    scope: str
    token_type: str
    expires_at: int


class DecodedAccessToken(BaseModel):
    access_token_response: AccessTokenResponse
    decoded_claims: dict[str, Any]


def fetch_jwks_and_verify_token(
    access_token: str, dbt_platform_url: str
) -> dict[str, Any]:
    jwks_url = f"{dbt_platform_url}/.well-known/jwks.json"
    jwks_client = _get_jwks_client(jwks_url)
    signing_key = jwks_client.get_signing_key_from_jwt(access_token)
    claims = jwt.decode(
        access_token,
        signing_key.key,
        algorithms=["RS256"],
        options={"verify_aud": False},
    )
    return claims
