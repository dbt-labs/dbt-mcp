from typing import Any

import jwt
from jwt import PyJWKClient
from pydantic import BaseModel


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
    jwks_client = PyJWKClient(jwks_url)
    signing_key = jwks_client.get_signing_key_from_jwt(access_token)
    claims = jwt.decode(
        access_token,
        signing_key.key,
        algorithms=["RS256"],
        options={"verify_aud": False},
    )
    return claims
