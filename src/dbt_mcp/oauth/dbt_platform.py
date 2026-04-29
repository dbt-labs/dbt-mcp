from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from dbt_mcp.oauth.token import (
    AccessTokenResponse,
    DecodedAccessToken,
    fetch_jwks_and_verify_token,
)


class DbtPlatformAccount(BaseModel):
    id: int
    name: str
    locked: bool
    state: int
    static_subdomain: str | None
    vanity_subdomain: str | None

    @property
    def host_prefix(self) -> str | None:
        if self.static_subdomain:
            return self.static_subdomain
        if self.vanity_subdomain:
            return self.vanity_subdomain
        return None


class DbtPlatformProject(BaseModel):
    id: int
    name: str
    description: str | None = None
    account_id: int
    account_name: str


class DbtPlatformEnvironmentResponse(BaseModel):
    id: int
    name: str
    type: str | None = None
    deployment_type: str | None = None


class DbtPlatformEnvironment(BaseModel):
    id: int
    name: str
    deployment_type: str


class SelectedProjectsRequest(BaseModel):
    account_id: int
    project_ids: list[int] = Field(min_length=1)


def dbt_platform_context_from_token_response(
    token_response: dict[str, Any], dbt_platform_url: str
) -> DbtPlatformContext:
    new_access_token_response = AccessTokenResponse(**token_response)
    decoded_claims = fetch_jwks_and_verify_token(
        new_access_token_response.access_token, dbt_platform_url
    )
    decoded_access_token = DecodedAccessToken(
        access_token_response=new_access_token_response,
        decoded_claims=decoded_claims,
    )
    return DbtPlatformContext(
        decoded_access_token=decoded_access_token,
    )


class DbtPlatformContext(BaseModel):
    decoded_access_token: DecodedAccessToken | None = None
    host_prefix: str | None = None
    dbt_host: str | None = None
    dev_environment: DbtPlatformEnvironment | None = None
    prod_environment: DbtPlatformEnvironment | None = None
    account_id: int | None = None
    selected_project_ids: list[int] | None = None

    @property
    def user_id(self) -> int | None:
        return (
            int(self.decoded_access_token.decoded_claims["sub"])
            if self.decoded_access_token
            else None
        )

    def override(self, other: DbtPlatformContext) -> DbtPlatformContext:
        # When transitioning to multi-project mode, environments from a prior
        # single-project login must not be carried forward — they would create
        # an invalid state where both dbt_prod_env_id and dbt_project_ids are set.
        inheriting_environments = not other.selected_project_ids
        return DbtPlatformContext(
            dev_environment=other.dev_environment
            or (self.dev_environment if inheriting_environments else None),
            prod_environment=other.prod_environment
            or (self.prod_environment if inheriting_environments else None),
            decoded_access_token=other.decoded_access_token
            or self.decoded_access_token,
            host_prefix=other.host_prefix or self.host_prefix,
            dbt_host=other.dbt_host or self.dbt_host,
            account_id=other.account_id or self.account_id,
            selected_project_ids=other.selected_project_ids
            or self.selected_project_ids,
        )
