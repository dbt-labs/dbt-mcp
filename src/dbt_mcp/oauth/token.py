from typing import Any

from pydantic import BaseModel


class AccessTokenResponse(BaseModel):
    access_token: str
    refresh_token: str | None = None
    expires_in: int
    scope: str
    token_type: str
    expires_at: int


class DecodedAccessToken(BaseModel):
    access_token_response: AccessTokenResponse
    decoded_claims: dict[str, Any]
