from contextlib import AbstractContextManager
from typing import Any, Protocol

import pyarrow as pa
from dbtsl.api.shared.query_params import (
    GroupByParam,
    OrderByGroupBy,
    OrderByMetric,
    OrderBySpec,
)
from dbtsl.client.sync import SyncSemanticLayerClient
from dbtsl.error import QueryFailedError

from dbt_mcp.config.config_providers import ConfigProvider, SemanticLayerConfig
from dbt_mcp.semantic_layer.gql.gql import GRAPHQL_QUERIES
from dbt_mcp.semantic_layer.gql.gql_request import submit_request
from dbt_mcp.semantic_layer.levenshtein import get_misspellings
from dbt_mcp.semantic_layer.types import (
    DimensionToolResponse,
    EntityToolResponse,
    GetMetricsCompiledSqlError,
    GetMetricsCompiledSqlResult,
    GetMetricsCompiledSqlSuccess,
    MetricToolResponse,
    OrderByParam,
    QueryMetricsError,
    QueryMetricsResult,
    QueryMetricsSuccess,
)


class SemanticLayerClientProtocol(Protocol):
    def session(self) -> AbstractContextManager[Any]: ...

    def query(
        self,
        metrics: list[str],
        group_by: list[GroupByParam | str] | None = None,
        limit: int | None = None,
        order_by: list[str | OrderByGroupBy | OrderByMetric] | None = None,
        where: list[str] | None = None,
        read_cache: bool = True,
    ) -> pa.Table: ...

    def compile_sql(
        self,
        metrics: list[str],
        group_by: list[str] | None = None,
        limit: int | None = None,
        order_by: list[str | OrderByGroupBy | OrderByMetric] | None = None,
        where: list[str] | None = None,
        read_cache: bool = True,
    ) -> str: ...


class SemanticLayerClientProvider(Protocol):
    async def get_client(self) -> SemanticLayerClientProtocol: ...


class DefaultSemanticLayerClientProvider:
    def __init__(self, config_provider: ConfigProvider[SemanticLayerConfig]):
        self.config_provider = config_provider

    async def get_client(self) -> SemanticLayerClientProtocol:
        config = await self.config_provider.get_config()
        return SyncSemanticLayerClient(
            environment_id=config.prod_environment_id,
            auth_token=config.token,
            host=config.host,
        )


class SemanticLayerFetcher:
    def __init__(
        self,
        config_provider: ConfigProvider[SemanticLayerConfig],
        client_provider: SemanticLayerClientProvider,
    ):
        self.client_provider = client_provider
        self.config_provider = config_provider
        self.entities_cache: dict[str, list[EntityToolResponse]] = {}
        self.dimensions_cache: dict[str, list[DimensionToolResponse]] = {}

    async def list_metrics(self, search: str | None = None) -> list[MetricToolResponse]:
        metrics_result = submit_request(
            await self.config_provider.get_config(),
            {"query": GRAPHQL_QUERIES["metrics"], "variables": {"search": search}},
        )
        return [
            MetricToolResponse(
                name=m.get("name"),
                type=m.get("type"),
                label=m.get("label"),
                description=m.get("description"),
                metadata=(m.get("config") or {}).get("meta", ""),
            )
            for m in metrics_result["data"]["metricsPaginated"]["items"]
        ]

    async def get_dimensions(
        self, metrics: list[str], search: str | None = None
    ) -> list[DimensionToolResponse]:
        metrics_key = ",".join(sorted(metrics))
        if metrics_key not in self.dimensions_cache:
            dimensions_result = submit_request(
                await self.config_provider.get_config(),
                {
                    "query": GRAPHQL_QUERIES["dimensions"],
                    "variables": {
                        "metrics": [{"name": m} for m in metrics],
                        "search": search,
                    },
                },
            )
            dimensions = []
            for d in dimensions_result["data"]["dimensionsPaginated"]["items"]:
                dimensions.append(
                    DimensionToolResponse(
                        name=d.get("name"),
                        type=d.get("type"),
                        description=d.get("description"),
                        label=d.get("label"),
                        granularities=d.get("queryableGranularities")
                        + d.get("queryableTimeGranularities"),
                    )
                )
            self.dimensions_cache[metrics_key] = dimensions
        return self.dimensions_cache[metrics_key]

    async def get_entities(
        self, metrics: list[str], search: str | None = None
    ) -> list[EntityToolResponse]:
        metrics_key = ",".join(sorted(metrics))
        if metrics_key not in self.entities_cache:
            entities_result = submit_request(
                await self.config_provider.get_config(),
                {
                    "query": GRAPHQL_QUERIES["entities"],
                    "variables": {
                        "metrics": [{"name": m} for m in metrics],
                        "search": search,
                    },
                },
            )
            entities = [
                EntityToolResponse(
                    name=e.get("name"),
                    type=e.get("type"),
                    description=e.get("description"),
                )
                for e in entities_result["data"]["entitiesPaginated"]["items"]
            ]
            self.entities_cache[metrics_key] = entities
        return self.entities_cache[metrics_key]

    async def get_metrics_compiled_sql(
        self,
        metrics: list[str],
        group_by: list[GroupByParam] | None = None,
        order_by: list[OrderByParam] | None = None,
        where: str | None = None,
        limit: int | None = None,
    ) -> GetMetricsCompiledSqlResult:
        """
        Get compiled SQL for the given metrics and group by parameters using the SDK.

        Args:
            metrics: List of metric names to get compiled SQL for
            group_by: List of group by parameters (dimensions/entities with optional grain)
            order_by: List of order by parameters
            where: Optional SQL WHERE clause to filter results
            limit: Optional limit for number of results

        Returns:
            GetMetricsCompiledSqlResult with either the compiled SQL or an error
        """
        validation_error = await self.validate_query_metrics_params(
            metrics=metrics,
            group_by=group_by,
        )
        if validation_error:
            return GetMetricsCompiledSqlError(error=validation_error)

        try:
            sl_client = await self.client_provider.get_client()
            with sl_client.session():
                parsed_order_by: list[OrderBySpec] = (
                    self.get_order_bys(
                        order_by=order_by, metrics=metrics, group_by=group_by
                    )
                    if order_by is not None
                    else []
                )

                compiled_sql = sl_client.compile_sql(
                    metrics=metrics,
                    group_by=group_by,  # type: ignore
                    order_by=parsed_order_by,  # type: ignore
                    where=[where] if where else None,
                    limit=limit,
                    read_cache=True,
                )

                return GetMetricsCompiledSqlSuccess(sql=compiled_sql)

        except Exception as e:
            return self._format_get_metrics_compiled_sql_error(e)

    def _format_semantic_layer_error(self, error: Exception) -> str:
        """Format semantic layer errors by cleaning up common error message patterns."""
        error_str = str(error)
        return (
            error_str.replace("QueryFailedError(", "")
            .rstrip(")")
            .lstrip("[")
            .rstrip("]")
            .lstrip('"')
            .rstrip('"')
            .replace("INVALID_ARGUMENT: [FlightSQL]", "")
            .replace("(InvalidArgument; Prepare)", "")
            .replace("(InvalidArgument; ExecuteQuery)", "")
            .replace("Failed to prepare statement:", "")
            .replace("com.dbt.semanticlayer.exceptions.DataPlatformException:", "")
            .strip()
        )

    def _format_get_metrics_compiled_sql_error(
        self, compile_error: Exception
    ) -> GetMetricsCompiledSqlError:
        """Format get compiled SQL errors using the shared error formatter."""
        return GetMetricsCompiledSqlError(
            error=self._format_semantic_layer_error(compile_error)
        )

    async def validate_query_metrics_params(
        self, metrics: list[str], group_by: list[GroupByParam] | None
    ) -> str | None:
        errors = []
        available_metrics_names = [m.name for m in await self.list_metrics()]
        metric_misspellings = get_misspellings(
            targets=metrics,
            words=available_metrics_names,
            top_k=5,
        )
        for metric_misspelling in metric_misspellings:
            recommendations = (
                " Did you mean: " + ", ".join(metric_misspelling.similar_words) + "?"
            )
            errors.append(
                f"Metric {metric_misspelling.word} not found."
                + (recommendations if metric_misspelling.similar_words else "")
            )

        if errors:
            return f"Errors: {', '.join(errors)}"

        available_group_by = [d.name for d in await self.get_dimensions(metrics)] + [
            e.name for e in await self.get_entities(metrics)
        ]
        group_by_misspellings = get_misspellings(
            targets=[g.name for g in group_by or []],
            words=available_group_by,
            top_k=5,
        )
        for group_by_misspelling in group_by_misspellings:
            recommendations = (
                " Did you mean: " + ", ".join(group_by_misspelling.similar_words) + "?"
            )
            errors.append(
                f"Group by {group_by_misspelling.word} not found."
                + (recommendations if group_by_misspelling.similar_words else "")
            )

        if errors:
            return f"Errors: {', '.join(errors)}"
        return None

    # TODO: move this to the SDK
    def _format_query_failed_error(self, query_error: Exception) -> QueryMetricsError:
        if isinstance(query_error, QueryFailedError):
            return QueryMetricsError(
                error=self._format_semantic_layer_error(query_error)
            )
        else:
            return QueryMetricsError(error=str(query_error))

    def get_order_bys(
        self,
        order_by: list[OrderByParam],
        metrics: list[str],
        group_by: list[GroupByParam] | None = None,
    ) -> list[OrderBySpec]:
        result: list[OrderBySpec] = []
        queried_group_by = {g.name: g for g in group_by} if group_by else {}
        queried_metrics = set(metrics)
        for o in order_by:
            if o.name in queried_metrics:
                result.append(OrderByMetric(name=o.name, descending=o.descending))
            elif o.name in queried_group_by:
                selected_group_by = queried_group_by[o.name]
                result.append(
                    OrderByGroupBy(
                        name=selected_group_by.name,
                        descending=o.descending,
                        grain=selected_group_by.grain,
                    )
                )
            else:
                raise ValueError(
                    f"Order by `{o.name}` not found in metrics or group by"
                )
        return result

    async def query_metrics(
        self,
        metrics: list[str],
        group_by: list[GroupByParam] | None = None,
        order_by: list[OrderByParam] | None = None,
        where: str | None = None,
        limit: int | None = None,
    ) -> QueryMetricsResult:
        validation_error = await self.validate_query_metrics_params(
            metrics=metrics,
            group_by=group_by,
        )
        if validation_error:
            return QueryMetricsError(error=validation_error)

        try:
            query_error = None
            sl_client = await self.client_provider.get_client()
            with sl_client.session():
                # Catching any exception within the session
                # to ensure it is closed properly
                try:
                    parsed_order_by: list[OrderBySpec] = (
                        self.get_order_bys(
                            order_by=order_by, metrics=metrics, group_by=group_by
                        )
                        if order_by is not None
                        else []
                    )
                    query_result = sl_client.query(
                        metrics=metrics,
                        # TODO: remove this type ignore once this PR is merged: https://github.com/dbt-labs/semantic-layer-sdk-python/pull/80
                        group_by=group_by,  # type: ignore
                        order_by=parsed_order_by,  # type: ignore
                        where=[where] if where else None,
                        limit=limit,
                    )
                except Exception as e:
                    query_error = e
            if query_error:
                return self._format_query_failed_error(query_error)
            json_result = query_result.to_pandas().to_json(orient="records", indent=2)
            return QueryMetricsSuccess(result=json_result or "")
        except Exception as e:
            return self._format_query_failed_error(e)
