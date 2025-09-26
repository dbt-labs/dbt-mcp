from pydantic import BaseModel, ConfigDict


class DbtProjectFlags(BaseModel):
    model_config = ConfigDict(extra="allow")
    send_anonymous_usage_stats: bool | None = None


class DbtProjectYaml(BaseModel):
    model_config = ConfigDict(extra="allow")
    flags: None | DbtProjectFlags = None
