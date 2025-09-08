from pydantic import BaseModel


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
    user_id: int
    host_prefix: str | None = None
    dev_environment: DbtPlatformEnvironment | None = None
    prod_environment: DbtPlatformEnvironment | None = None

    def override(self, other: "DbtPlatformContext") -> "DbtPlatformContext":
        return DbtPlatformContext(
            dev_environment=other.dev_environment or self.dev_environment,
            prod_environment=other.prod_environment or self.prod_environment,
            user_id=other.user_id or self.user_id,
            host_prefix=other.host_prefix or self.host_prefix,
        )
