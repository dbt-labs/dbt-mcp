import asyncio
import base64
import json
import logging
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Protocol

import pyarrow as pa
from dbtsl.api.shared.query_params import (
    GroupByParam,
    OrderByGroupBy,
    OrderByMetric,
    OrderBySpec,
)
from dbtsl.client.sync import SyncSemanticLayerClient
from dbtsl.error import QueryFailedError, RetryTimeoutError
from dbtsl.models.query import QueryStatus

from dbt_mcp.config.config_providers import SemanticLayerConfig
from dbt_mcp.errors import InvalidParameterError
from dbt_mcp.errors.semantic_layer import SemanticLayerQueryTimeoutError
from dbt_mcp.semantic_layer.gql.gql import GRAPHQL_QUERIES
from dbt_mcp.semantic_layer.gql.gql_request import submit_request
from dbt_mcp.semantic_layer.types import (
    DimensionToolResponse,
    EntityToolResponse,
    GetMetricsCompiledSqlError,
    GetMetricsCompiledSqlResult,
    GetMetricsCompiledSqlSuccess,
    ListMetricsResponse,
    MetricToolResponse,
    OrderByParam,
    QueryMetricsError,
    QueryMetricsResult,
    QueryMetricsSuccess,
    SavedQueryToolResponse,
)

logger = logging.getLogger(__name__)


def DEFAULT_RESULT_FORMATTER(table: pa.Table) -> str:
    """Convert PyArrow Table to JSON string with ISO date formatting.

    This replaces the pandas-based implementation with native PyArrow and Python json.
    Output format: array of objects (records), 2-space indentation, ISO date strings.
    """
    # Convert PyArrow table to list of dictionaries
    records = table.to_pylist()

    # Custom JSON encoder to handle date/datetime, time, Decimal, timedelta, and bytes objects
    class ExtendedJSONEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, datetime | date):
                return obj.isoformat()
            if isinstance(obj, time):
                return obj.isoformat()
            if isinstance(obj, Decimal):
                return float(obj)
            if isinstance(obj, timedelta):
                return obj.total_seconds()
            if isinstance(obj, bytes):
                return base64.b64encode(obj).decode("utf-8")
            return super().default(obj)

    # Return JSON with records format and proper indentation
    return json.dumps(records, indent=2, cls=ExtendedJSONEncoder)


# Cap the number of substrings accepted by `list_metrics(search=[...])` so
# an unbounded LLM-supplied list can't fan out into a burst of parallel
# GraphQL requests against the Semantic Layer API.
_MAX_SEARCH_TERMS = 20


def _dedupe_metric_items(items: Any) -> list[Any]:
    """Preserve first-seen order while filtering out duplicate metric names."""
    seen: set[str] = set()
    out: list[Any] = []
    for item in items:
        name = item.get("name")
        if name is None or name in seen:
            continue
        seen.add(name)
        out.append(item)
    return out


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
    async def get_client(
        self, *, config: SemanticLayerConfig
    ) -> SemanticLayerClientProtocol: ...


class DefaultSemanticLayerClientProvider:
    async def get_client(
        self, *, config: SemanticLayerConfig
    ) -> SemanticLayerClientProtocol:
        return SyncSemanticLayerClient(
            environment_id=config.prod_environment_id,
            auth_token=config.token_provider.get_token(),
            host=config.host,
        )


class SemanticLayerFetcher:
    def __init__(
        self,
        client_provider: SemanticLayerClientProvider,
    ):
        self.client_provider = client_provider
        # TODO: we shouldn't allow these dicts to grow unbounded?
        self.entities_cache: dict[tuple[str, str | None], list[EntityToolResponse]] = {}
        self.dimensions_cache: dict[
            tuple[str, str | None], list[DimensionToolResponse]
        ] = {}

    async def list_metrics(
        self,
        config: SemanticLayerConfig,
        search: str | list[str] | None = None,
    ) -> ListMetricsResponse:
        # `search` may be a single substring or a list of substrings; for a list
        # we fan out one GraphQL call per substring, then merge & dedupe by name.
        search_terms: list[str | None]
        if isinstance(search, list):
            # Strip whitespace, drop empty values, and dedupe identical terms
            # (preserving first-seen order) so a whitespace-only or duplicated
            # term can't broaden the fan-out into redundant or no-filter calls.
            cleaned: list[str] = []
            seen_terms: set[str] = set()
            for raw in search:
                term = raw.strip()
                if not term or term in seen_terms:
                    continue
                seen_terms.add(term)
                cleaned.append(term)
            if len(cleaned) > _MAX_SEARCH_TERMS:
                # Cap the fan-out so a runaway LLM call can't generate an
                # unbounded burst of parallel GraphQL requests.
                raise InvalidParameterError(
                    f"`search` accepts at most {_MAX_SEARCH_TERMS} terms; "
                    f"got {len(cleaned)}."
                )
            search_terms = list(cleaned) if cleaned else [None]
        else:
            # Mirror the list-path normalization for parity: a single-string
            # `search` is stripped, and an empty/whitespace-only string becomes
            # no filter (search=None).
            normalized = search.strip() if isinstance(search, str) else search
            search_terms = [normalized if normalized else None]

        cheap_results = await asyncio.gather(
            *(
                submit_request(
                    config,
                    {
                        "query": GRAPHQL_QUERIES["metrics"],
                        "variables": {"search": term},
                    },
                )
                for term in search_terms
            )
        )
        cheap_items = _dedupe_metric_items(
            item
            for r in cheap_results
            for item in r["data"]["metricsPaginated"]["items"]
        )
        dimensionless_response = ListMetricsResponse(
            metrics=[
                MetricToolResponse(
                    name=m.get("name"),
                    type=m.get("type"),
                    label=m.get("label"),
                    description=m.get("description"),
                    metadata=(m.get("config") or {}).get("meta"),
                )
                for m in cheap_items
            ]
        )

        if cheap_items and len(cheap_items) <= config.metrics_related_max:
            # Re-fetch with per-metric dimensions and entities. Same fan-out:
            # the nested GQL fields return per-metric data accurately, unlike
            # dimensionsPaginated with multiple metrics which would intersect.
            # Fall back to the dimensionless response if the richer query
            # times out or otherwise fails (one slow term would otherwise
            # block the whole call via asyncio.gather).
            try:
                related_results = await asyncio.gather(
                    *(
                        submit_request(
                            config,
                            {
                                "query": GRAPHQL_QUERIES["metrics_with_related"],
                                "variables": {"search": term},
                            },
                            timeout=5.0,
                        )
                        for term in search_terms
                    )
                )
            except Exception as e:
                logger.warning(f"Error fetching metrics with related: {e}")
                return dimensionless_response
            related_items = _dedupe_metric_items(
                item
                for r in related_results
                for item in r["data"]["metricsPaginated"]["items"]
            )
            return ListMetricsResponse(
                metrics=[
                    MetricToolResponse(
                        name=m.get("name"),
                        type=m.get("type"),
                        label=m.get("label"),
                        description=m.get("description"),
                        metadata=(m.get("config") or {}).get("meta"),
                        dimensions=[d.get("name") for d in (m.get("dimensions") or [])],
                        entities=[e.get("name") for e in (m.get("entities") or [])],
                    )
                    for m in related_items
                ]
            )
        return dimensionless_response

    async def list_saved_queries(
        self,
        config: SemanticLayerConfig,
        search: str | None = None,
    ) -> list[SavedQueryToolResponse]:
        """Fetch all saved queries from the Semantic Layer API."""
        saved_queries_result = await submit_request(
            config,
            {
                "query": GRAPHQL_QUERIES["saved_queries"],
                "variables": {"search": search},
            },
        )
        return [
            SavedQueryToolResponse(
                name=sq.get("name"),
                label=sq.get("label"),
                description=sq.get("description"),
                metrics=[
                    m.get("name") for m in sq.get("queryParams", {}).get("metrics", [])
                ]
                if sq.get("queryParams", {}).get("metrics")
                else None,
                group_by=[
                    g.get("name") for g in sq.get("queryParams", {}).get("groupBy", [])
                ]
                if sq.get("queryParams", {}).get("groupBy")
                else None,
                where=sq.get("queryParams", {}).get("where", {}).get("whereSqlTemplate")
                if sq.get("queryParams", {}).get("where")
                else None,
            )
            for sq in saved_queries_result["data"]["savedQueriesPaginated"]["items"]
        ]

    async def get_dimensions(
        self,
        config: SemanticLayerConfig,
        metrics: list[str],
        search: str | None = None,
    ) -> list[DimensionToolResponse]:
        metrics_key = (",".join(sorted(metrics)), search)
        if metrics_key not in self.dimensions_cache:
            dimensions_result = await submit_request(
                config,
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
                        metadata=(d.get("config") or {}).get("meta"),
                    )
                )
            self.dimensions_cache[metrics_key] = dimensions
        return self.dimensions_cache[metrics_key]

    async def get_entities(
        self,
        config: SemanticLayerConfig,
        metrics: list[str],
        search: str | None = None,
    ) -> list[EntityToolResponse]:
        metrics_key = (",".join(sorted(metrics)), search)
        if metrics_key not in self.entities_cache:
            entities_result = await submit_request(
                config,
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

    def _format_semantic_layer_error(self, error: Exception) -> str:
        """Format semantic layer errors by cleaning up common error message patterns."""
        error_str = str(error)
        formatted = (
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
        if not formatted:
            return error_str or f"Semantic layer query failed: {type(error).__name__}"
        return formatted

    def _format_get_metrics_compiled_sql_error(
        self, compile_error: Exception
    ) -> GetMetricsCompiledSqlError:
        """Format get compiled SQL errors using the shared error formatter."""
        return GetMetricsCompiledSqlError(
            error=self._format_semantic_layer_error(compile_error)
        )

    def _normalize_where(self, where: str | None) -> str | None:
        """Strip surrounding quotes that LLMs sometimes add to where clause strings.

        Returns None if the input is None or becomes empty/whitespace-only after
        stripping quotes — the caller should treat this as "no where clause".
        """
        if where is None:
            return None
        where = where.strip()
        if len(where) >= 2 and where[0] == '"' and where[-1] == '"':
            where = where[1:-1]
        return where.strip() or None

    # TODO: move this to the SDK
    def _format_query_failed_error(self, query_error: Exception) -> QueryMetricsError:
        if isinstance(query_error, QueryFailedError):
            return QueryMetricsError(
                error=self._format_semantic_layer_error(query_error)
            )
        else:
            return QueryMetricsError(error=str(query_error))

    def _get_order_bys(
        self,
        order_by: list[OrderByParam] | None,
        metrics: list[str] = [],
        group_by: list[GroupByParam] | None = None,
    ) -> list[OrderBySpec]:
        result: list[OrderBySpec] = []
        if order_by is None:
            return result
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
                raise InvalidParameterError(
                    f"Order by `{o.name}` not found in metrics or group by"
                )
        return result

    async def get_metrics_compiled_sql(
        self,
        config: SemanticLayerConfig,
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
        try:
            sl_client = await self.client_provider.get_client(
                config=config,
            )
            with sl_client.session():
                parsed_order_by: list[OrderBySpec] = self._get_order_bys(
                    order_by=order_by, metrics=metrics, group_by=group_by
                )
                compiled_sql = await asyncio.to_thread(
                    sl_client.compile_sql,
                    metrics=metrics,
                    group_by=group_by,  # type: ignore
                    order_by=parsed_order_by,  # type: ignore
                    where=[normalized_where]
                    if (normalized_where := self._normalize_where(where))
                    else None,
                    limit=limit,
                    read_cache=True,
                )

                return GetMetricsCompiledSqlSuccess(sql=compiled_sql)

        except Exception as e:
            return self._format_get_metrics_compiled_sql_error(e)

    async def query_metrics(
        self,
        config: SemanticLayerConfig,
        metrics: list[str],
        group_by: list[GroupByParam] | None = None,
        order_by: list[OrderByParam] | None = None,
        where: str | None = None,
        limit: int | None = None,
        result_formatter: Callable[[pa.Table], str] | None = None,
    ) -> QueryMetricsResult:
        try:
            query_error: Exception | None = None
            sl_client = await self.client_provider.get_client(
                config=config,
            )
            with sl_client.session():
                # Catching any exception within the session
                # to ensure it is closed properly
                try:
                    parsed_order_by: list[OrderBySpec] = self._get_order_bys(
                        order_by=order_by, metrics=metrics, group_by=group_by
                    )
                    query_result = await asyncio.to_thread(
                        sl_client.query,
                        metrics=metrics,
                        group_by=group_by,  # type: ignore
                        order_by=parsed_order_by,  # type: ignore
                        where=[normalized_where]
                        if (normalized_where := self._normalize_where(where))
                        else None,
                        limit=limit,
                    )
                except RetryTimeoutError as e:
                    # Queries that timeout with COMPILED status have finished SQL
                    # compilation and are executing against the data platform. In
                    # agent contexts, this indicates the query is too complex and
                    # the client should request a simpler query.
                    if e.status == QueryStatus.COMPILED.value:
                        raise SemanticLayerQueryTimeoutError(
                            f"The semantic layer query timed out after {e.timeout_s}s while "
                            f"executing against the data platform (status: COMPILED). This "
                            f"indicates the query is too complex or returns too much data "
                            f"for an agent context. Please simplify the query by adding "
                            f"filters, reducing dimensions, or limiting results."
                        ) from e
                    query_error = e
                except Exception as e:
                    query_error = e
            if query_error:
                return self._format_query_failed_error(query_error)
            formatter = result_formatter or DEFAULT_RESULT_FORMATTER
            json_result = formatter(query_result)
            return QueryMetricsSuccess(result=json_result or "")
        except SemanticLayerQueryTimeoutError:
            raise
        except Exception as e:
            return self._format_query_failed_error(e)
