import re
from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict, Field


@dataclass
class SemVer:
    major: int
    minor: int
    patch: str  # str to handle pre-release suffixes like "0b1"

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


class DbtProjectFlags(BaseModel):
    model_config = ConfigDict(extra="allow")
    send_anonymous_usage_stats: bool | None = None


class DbtProjectYaml(BaseModel):
    model_config = ConfigDict(extra="allow")
    flags: None | DbtProjectFlags = None
    require_dbt_version: str | list[str] | None = Field(
        None, alias="require-dbt-version"
    )


_DBT_VERSION_RE = re.compile(r"(\d+)\.(\d+)(?:\.(\S+))?")


def parse_dbt_version_minor(
    require_dbt_version: str | list[str] | None,
) -> SemVer | None:
    """Extract the minimum dbt version from a ``require-dbt-version`` spec.

    Handles common forms: ``">=1.8.0"``, ``"1.8"``, ``[">=1.8.0", "<2.0"]``.
    Returns ``None`` when the version cannot be determined.

    Examples::

        parse_dbt_version_minor(">=1.8.0")           # SemVer(major=1, minor=8, patch="0")
        parse_dbt_version_minor("1.8")                # SemVer(major=1, minor=8, patch="0")
        parse_dbt_version_minor([">=1.8.0", "<2.0"]) # SemVer(major=1, minor=8, patch="0")
    """
    if require_dbt_version is None:
        return None
    raw = (
        require_dbt_version
        if isinstance(require_dbt_version, str)
        else require_dbt_version[0]
    )
    match = _DBT_VERSION_RE.search(raw)
    if not match:
        return None
    return SemVer(
        major=int(match.group(1)),
        minor=int(match.group(2)),
        patch=match.group(3) or "0",
    )
