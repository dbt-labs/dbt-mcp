from functools import cache

from dbtsl.api.shared.query_params import OrderByGroupBy
from dbtsl.error import QueryFailedError

from dbt_mcp.config.config import Config
from dbt_mcp.semantic_layer.gql.gql import GRAPHQL_QUERIES
from dbt_mcp.semantic_layer.gql.gql_request import ConnAttr, submit_request
from dbt_mcp.semantic_layer.levenshtein import get_misspellings
from dbt_mcp.semantic_layer.sdk.query_params import GroupByParam
from dbt_mcp.semantic_layer.sdk.sync_sl_client import SyncSemanticLayerClient
from dbt_mcp.semantic_layer.types import (
    DimensionToolResponse,
    EntityToolResponse,
    MetricToolResponse,
    OrderByParam,
    QueryMetricsError,
    QueryMetricsResult,
    QueryMetricsSuccess,
)


class SemanticLayerFetcher:
    def __init__(self, sl_client: SyncSemanticLayerClient, host: str, config: Config):
        self.sl_client = sl_client
        self.host = host
        self.config = config
        self.entities_cache: dict[str, list[EntityToolResponse]] = {}
        self.dimensions_cache: dict[str, list[DimensionToolResponse]] = {}

    @cache
    def list_metrics(self) -> list[MetricToolResponse]:
        metrics_result = submit_request(
            ConnAttr(
                host=self.host,
                params={"environmentid": self.config.environment_id},
                auth_header=f"Bearer {self.config.token}",
            ),
            {"query": GRAPHQL_QUERIES["metrics"]},
        )
        if "errors" in metrics_result:
            raise ValueError(metrics_result["errors"])
        return [
            MetricToolResponse(
                name=m.get("name"),
                type=m.get("type"),
                label=m.get("label"),
                description=m.get("description"),
            )
            for m in metrics_result["data"]["metrics"]
        ]

    def get_dimensions(self, metrics: list[str]) -> list[DimensionToolResponse]:
        metrics_key = ",".join(sorted(metrics))
        if metrics_key not in self.dimensions_cache:
            dimensions_result = submit_request(
                ConnAttr(
                    host=self.host,
                    params={"environmentid": self.config.environment_id},
                    auth_header=f"Bearer {self.config.token}",
                ),
                {
                    "query": GRAPHQL_QUERIES["dimensions"],
                    "variables": {"metrics": [{"name": m} for m in metrics]},
                },
            )
            if "errors" in dimensions_result:
                raise ValueError(dimensions_result["errors"])
            dimensions = []
            for d in dimensions_result["data"]["dimensions"]:
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

    def get_entities(self, metrics: list[str]) -> list[EntityToolResponse]:
        metrics_key = ",".join(sorted(metrics))
        if metrics_key not in self.entities_cache:
            entities_result = submit_request(
                ConnAttr(
                    host=self.host,
                    params={"environmentid": self.config.environment_id},
                    auth_header=f"Bearer {self.config.token}",
                ),
                {
                    "query": GRAPHQL_QUERIES["entities"],
                    "variables": {"metrics": [{"name": m} for m in metrics]},
                },
            )
            if "errors" in entities_result:
                raise ValueError(entities_result["errors"])
            entities = [
                EntityToolResponse(
                    name=e.get("name"),
                    type=e.get("type"),
                    description=e.get("description"),
                )
                for e in entities_result["data"]["entities"]
            ]
            self.entities_cache[metrics_key] = entities
        return self.entities_cache[metrics_key]

    def validate_query_metrics_params(
        self, metrics: list[str], group_by: list[GroupByParam] | None
    ) -> str | None:
        errors = []
        available_metrics_names = [m.name for m in self.list_metrics()]
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
                f"Metric {metric_misspelling.word} not found." + recommendations
                if metric_misspelling.similar_words
                else ""
            )

        if errors:
            return f"Errors: {', '.join(errors)}"

        available_dimensions = [d.name for d in self.get_dimensions(metrics)]
        dimension_misspellings = get_misspellings(
            targets=[g.name for g in group_by or []],
            words=available_dimensions,
            top_k=5,
        )
        for dimension_misspelling in dimension_misspellings:
            recommendations = (
                " Did you mean: " + ", ".join(dimension_misspelling.similar_words) + "?"
            )
            errors.append(
                f"Dimension {dimension_misspelling.word} not found." + recommendations
                if dimension_misspelling.similar_words
                else ""
            )

        if errors:
            return f"Errors: {', '.join(errors)}"
        return None

    # TODO: move this to the SDK
    def _format_query_failed_error(self, query_error: Exception) -> QueryMetricsError:
        if isinstance(query_error, QueryFailedError):
            return QueryMetricsError(
                error=str(query_error)
                .replace("QueryFailedError(", "")
                .rstrip(")")
                .lstrip("[")
                .rstrip("]")
                .lstrip('"')
                .rstrip('"')
                .replace("INVALID_ARGUMENT: [FlightSQL]", "")
                .replace("(InvalidArgument; Prepare)", "")
                .replace("(InvalidArgument; ExecuteQuery)", "")
                .replace("Failed to prepare statement:", "")
                .replace(
                    "com.dbt.semanticlayer.exceptions.DataPlatformException:",
                    "",
                )
                .strip()
            )
        else:
            return QueryMetricsError(error=str(query_error))

    def query_metrics(
        self,
        metrics: list[str],
        group_by: list[GroupByParam] | None = None,
        order_by: list[OrderByParam] | None = None,
        where: str | None = None,
        limit: int | None = None,
    ) -> QueryMetricsResult:
        validation_error = self.validate_query_metrics_params(
            metrics=metrics,
            group_by=group_by,
        )
        if validation_error:
            return QueryMetricsError(error=validation_error)

        try:
            query_error = None
            with self.sl_client.session():
                # Catching any exception within the session
                # to ensure it is closed properly
                try:
                    query_result = self.sl_client.query(
                        metrics=metrics,
                        # TODO: remove this type ignore once this PR is merged: https://github.com/dbt-labs/semantic-layer-sdk-python/pull/80
                        group_by=group_by,  # type: ignore
                        order_by=[
                            OrderByGroupBy(
                                name=o.name,
                                descending=o.descending,
                                grain=None,
                            )
                            for o in order_by or []
                        ],
                        where=[where] if where else None,
                        limit=limit,
                    )
                except Exception as e:
                    query_error = e
            if query_error:
                return self._format_query_failed_error(query_error)
            json_result = query_result.to_pandas().to_json(orient="records", indent=2)
            return QueryMetricsSuccess(result=json_result)
        except Exception as e:
            return self._format_query_failed_error(e)


def get_semantic_layer_fetcher(config: Config) -> SemanticLayerFetcher:
    is_local = config.host and config.host.startswith("localhost")
    if is_local:
        host = config.host
    elif config.multicell_account_prefix:
        host = f"{config.multicell_account_prefix}.semantic-layer.{config.host}"
    else:
        host = f"semantic-layer.{config.host}"
    if config.environment_id is None:
        raise ValueError("Environment ID is required")
    if config.token is None:
        raise ValueError("Token is required")
    assert host is not None

    semantic_layer_client = SyncSemanticLayerClient(
        environment_id=config.environment_id,
        auth_token=config.token,
        host=host,
    )

    return SemanticLayerFetcher(
        sl_client=semantic_layer_client,
        host=f"http://{host}" if is_local else f"https://{host}",
        config=config,
    )
