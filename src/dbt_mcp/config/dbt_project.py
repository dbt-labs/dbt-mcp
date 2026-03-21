import re

from pydantic import BaseModel, ConfigDict, Field


class DbtProjectFlags(BaseModel):
    model_config = ConfigDict(extra="allow")
    send_anonymous_usage_stats: bool | None = None


class DbtProjectYaml(BaseModel):
    model_config = ConfigDict(extra="allow")
    flags: None | DbtProjectFlags = None
    require_dbt_version: str | list[str] | None = Field(
        None, alias="require-dbt-version"
    )


_DBT_MINOR_VERSION_RE = re.compile(r"1\.(\d+)")


def parse_dbt_version_minor(require_dbt_version: str | list[str] | None) -> str | None:
    """Extract the minimum dbt minor version (e.g. ``"1.8"``) from a ``require-dbt-version`` spec.

    Handles common forms: ``">=1.8.0"``, ``"1.8"``, ``[">=1.8.0", "<2.0"]``.
    Returns ``None`` when the version cannot be determined.
    """
    if require_dbt_version is None:
        return None
    raw = (
        require_dbt_version
        if isinstance(require_dbt_version, str)
        else require_dbt_version[0]
    )
    match = _DBT_MINOR_VERSION_RE.search(raw)
    return f"1.{match.group(1)}" if match else None
