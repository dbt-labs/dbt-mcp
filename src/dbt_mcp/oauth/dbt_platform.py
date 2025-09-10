from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel

from dbt_mcp.oauth.token import DecodedAccessToken


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
    account_id: int
    account_name: str


class DbtPlatformEnvironmentResponse(BaseModel):
    id: int
    name: str
    deployment_type: str | None


class DbtPlatformEnvironment(BaseModel):
    id: int
    name: str
    deployment_type: str


class SelectedProjectRequest(BaseModel):
    account_id: int
    project_id: int


class DbtPlatformContext(BaseModel):
    decoded_access_token: DecodedAccessToken | None = None
    host_prefix: str | None = None
    dev_environment: DbtPlatformEnvironment | None = None
    prod_environment: DbtPlatformEnvironment | None = None

    @classmethod
    def from_file(cls, config_location: Path) -> DbtPlatformContext | None:
        try:
            return cls(**yaml.safe_load(config_location.read_text()))
        except Exception:
            return None

    @property
    def token(self) -> str | None:
        return (
            self.decoded_access_token.access_token_response.access_token
            if self.decoded_access_token
            else None
        )

    @property
    def user_id(self) -> int | None:
        return (
            int(self.decoded_access_token.decoded_claims["sub"])
            if self.decoded_access_token
            else None
        )

    def override(self, other: DbtPlatformContext) -> DbtPlatformContext:
        return DbtPlatformContext(
            dev_environment=other.dev_environment or self.dev_environment,
            prod_environment=other.prod_environment or self.prod_environment,
            decoded_access_token=other.decoded_access_token
            or self.decoded_access_token,
            host_prefix=other.host_prefix or self.host_prefix,
        )
