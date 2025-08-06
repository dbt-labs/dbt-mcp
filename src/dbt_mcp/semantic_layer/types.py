from dataclasses import dataclass

from dbtsl.models.dimension import DimensionType
from dbtsl.models.entity import EntityType
from dbtsl.models.metric import MetricType


@dataclass
class OrderByParam:
    name: str
    descending: bool


@dataclass
class MetricToolResponse:
    name: str
    type: MetricType
    label: str | None = None
    description: str | None = None


@dataclass
class DimensionToolResponse:
    name: str
    type: DimensionType
    description: str | None = None
    label: str | None = None
    granularities: list[str] | None = None


@dataclass
class EntityToolResponse:
    name: str
    type: EntityType
    description: str | None = None


@dataclass
class QueryMetricsSuccess:
    result: str
    error: None = None


@dataclass
class QueryMetricsError:
    error: str
    result: None = None


QueryMetricsResult = QueryMetricsSuccess | QueryMetricsError


@dataclass
class GetMetricsCompiledSqlSuccess:
    sql: str
    error: None = None


@dataclass
class GetMetricsCompiledSqlError:
    error: str
    sql: None = None


GetMetricsCompiledSqlResult = GetMetricsCompiledSqlSuccess | GetMetricsCompiledSqlError
