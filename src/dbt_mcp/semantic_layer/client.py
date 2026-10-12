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
from adbc_driver_flightsql import DatabaseOptions
from adbc_driver_manager import AdbcStatusCode, OperationalError
from dbtsl import env as dbtsl_env
from dbtsl.api.adbc.client.sync import SyncADBCClient
from dbtsl.api.graphql.client.sync import SyncGraphQLClient
from dbtsl.api.shared.query_params import (
    GroupByParam,
    OrderByGroupBy,
    OrderByMetric,
    OrderBySpec,
)
from dbtsl.client.base import BaseSemanticLayerClient
from dbtsl.client.sync import SyncSemanticLayerClient
from dbtsl.error import QueryFailedError, RetryTimeoutError
from dbtsl.models.query import QueryStatus

from dbt_mcp.config.config_providers import SemanticLayerConfig
from dbt_mcp.errors import InvalidParameterError, UpstreamResponseError
from dbt_mcp.errors.hints import classify_warehouse_error, warehouse_error_hint
from dbt_mcp.errors.semantic_layer import SemanticLayerQueryTimeoutError
from dbt_mcp.errors.warehouse_auth import append_hint, is_warehouse_auth_error
from dbt_mcp.semantic_layer.gql.gql import GRAPHQL_QUERIES
from dbt_mcp.semantic_layer.gql.gql_request import submit_request
from dbt_mcp.semantic_layer.types import (
    DimensionToolResponse,
    DimensionValuesError,
    DimensionValuesResult,
    DimensionValuesResponse,
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

from dbt_mcp.pagination import (
    ResultPage,
    numbered_pagination,
    validate_page_number,
    validate_page_size,
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

# Longest a Flight SQL action call, which includes preparing a statement, may
# take before it fails. Preparing compiles the query and prepares a statement on
# the data platform, so this is well above normal latency: about 99.97% of calls
# finish sooner. Without a limit, a call whose packets are silently dropped waits
# for the OS to give up, which takes about 16 minutes.
_FLIGHT_SQL_ACTION_TIMEOUT_SECONDS = 60

# ADBC status codes for a Flight SQL call that could not complete because of
# the connection rather than the query. `query_metrics` retries these over
# GraphQL; every other ADBC error propagates.
_CONNECTION_FAILURE_STATUS_CODES = (AdbcStatusCode.IO, AdbcStatusCode.TIMEOUT)


class _TimeBoundedFlightSqlClient(SyncADBCClient):
    @classmethod
    def _extra_db_kwargs(cls) -> dict[str, str]:
        return {
            **super()._extra_db_kwargs(),
            DatabaseOptions.TIMEOUT_UPDATE.value: str(
                _FLIGHT_SQL_ACTION_TIMEOUT_SECONDS
            ),
        }


class _TimeBoundedSemanticLayerClient(SyncSemanticLayerClient):
    """A `SyncSemanticLayerClient` whose Flight SQL calls have a time limit."""

    def __init__(self, environment_id: int, auth_token: str, host: str) -> None:
        # Skips `SyncSemanticLayerClient.__init__`, which fixes the Flight SQL
        # client class, to pass `_TimeBoundedFlightSqlClient` instead. The SDK's
        # own subclass needs the same type ignore for its generic parameters.
        BaseSemanticLayerClient.__init__(  # type: ignore[type-var]
            self,  # type: ignore[arg-type]
            environment_id=environment_id,
            auth_token=auth_token,
            host=host,
            gql_factory=SyncGraphQLClient,
            adbc_factory=_TimeBoundedFlightSqlClient,
            lazy=False,
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

    def dimension_values(
        self,
        metrics: list[str],
        group_by: str,
    ) -> Any: ...


class SemanticLayerQueryClientProtocol(Protocol):
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


class SemanticLayerClientProvider(Protocol):
    async def get_client(
        self, *, config: SemanticLayerConfig, time_bounded: bool = False
    ) -> SemanticLayerClientProtocol: ...

    async def get_graphql_client(
        self, *, config: SemanticLayerConfig
    ) -> SemanticLayerQueryClientProtocol: ...


class DefaultSemanticLayerClientProvider:
    async def get_client(
        self, *, config: SemanticLayerConfig, time_bounded: bool = False
    ) -> SemanticLayerClientProtocol:
        client_class = (
            _TimeBoundedSemanticLayerClient if time_bounded else SyncSemanticLayerClient
        )
        return client_class(
            environment_id=config.prod_environment_id,
            auth_token=config.token_provider.get_token(),
            host=config.host,
        )

    async def get_graphql_client(
        self, *, config: SemanticLayerConfig
    ) -> SemanticLayerQueryClientProtocol:
        return SyncGraphQLClient(
            environment_id=config.prod_environment_id,
            auth_token=config.token_provider.get_token(),
            server_host=config.host,
            url_format=dbtsl_env.GRAPHQL_URL_FORMAT,
            lazy=True,
        )


class SemanticLayerFetcher:
    def __init__(
        self,
        client_provider: SemanticLayerClientProvider,
    ):
        self.client_provider = client_provider

    async def list_metrics(
        self,
        config: SemanticLayerConfig,
        search: str | list[str] | None = None,
        *,
        page_num: int = 1,
        page_size: int = 50,
    ) -> ListMetricsResponse:
        validate_page_size(page_size)
        validate_page_number(page_num)
        if isinstance(search, list):
            if len(search) > _MAX_SEARCH_TERMS:
                raise InvalidParameterError(
                    f"`search` accepts at most {_MAX_SEARCH_TERMS} terms; got {len(search)}."
                )
            search_variables: dict[str, Any] = {
                "searchTerms": list(
                    dict.fromkeys(term.strip() for term in search if term.strip())
                )
            }
        else:
            search_variables = {
                "search": search.strip() or None if isinstance(search, str) else None
            }
        variables = search_variables | {"pageNum": page_num, "pageSize": page_size}
        cheap_result = await submit_request(
            config, {"query": GRAPHQL_QUERIES["metrics"], "variables": variables}
        )
        cheap_page = cheap_result["data"]["metricsPaginated"]
        cheap_items = cheap_page["items"]
        if len(cheap_items) > page_size:
            raise UpstreamResponseError(
                "Semantic Layer exceeded the requested metric page size."
            )
        pagination = numbered_pagination(cheap_page)
        dimensionless_response = ListMetricsResponse(
            pagination=pagination,
            metrics=[
                MetricToolResponse(
                    name=m.get("name"),
                    type=m.get("type"),
                    label=m.get("label"),
                    description=m.get("description"),
                    metadata=(m.get("config") or {}).get("meta"),
                )
                for m in cheap_items
            ],
        )

        if cheap_items and len(cheap_items) <= config.metrics_related_max:
            try:
                related_result = await submit_request(
                    config,
                    {
                        "query": GRAPHQL_QUERIES["metrics_with_related"],
                        "variables": variables,
                    },
                    timeout=5.0,
                )
                related_items = related_result["data"]["metricsPaginated"]["items"]
                if len(related_items) > page_size:
                    raise UpstreamResponseError(
                        "Semantic Layer exceeded the requested metric page size."
                    )
                related_by_name = {item["name"]: item for item in related_items}
                # Enrichment must not replace the chosen page if the catalog changes
                # between requests. Preserve its membership, order and totals.
                for metric in dimensionless_response.metrics:
                    related = related_by_name.get(metric.name)
                    if related is not None:
                        metric.dimensions = [
                            dimension["name"]
                            for dimension in related.get("dimensions") or []
                        ]
                        metric.entities = [
                            entity["name"] for entity in related.get("entities") or []
                        ]
            except Exception:
                logger.warning("Error fetching metrics with related", exc_info=True)
        return dimensionless_response

    async def list_saved_queries(
        self,
        config: SemanticLayerConfig,
        search: str | None = None,
        *,
        page_num: int = 1,
        page_size: int = 50,
    ) -> ResultPage[list[SavedQueryToolResponse]]:
        validate_page_size(page_size)
        validate_page_number(page_num)
        """Fetch all saved queries from the Semantic Layer API."""
        saved_queries_result = await submit_request(
            config,
            {
                "query": GRAPHQL_QUERIES["saved_queries"],
                "variables": {"pageNum": page_num, "pageSize": page_size},
            },
        )
        simple_results = [
            SavedQueryToolResponse(
                name=sq.get("name"),
                label=sq.get("label"),
                description=sq.get("description"),
            )
            for sq in saved_queries_result["data"]["savedQueriesPaginated"]["items"]
        ]
        try:
            full_result = await submit_request(
                config,
                {
                    "query": GRAPHQL_QUERIES["saved_queries_with_params"],
                    "variables": {"pageNum": page_num, "pageSize": page_size},
                },
                timeout=5.0,
            )
            results = [
                SavedQueryToolResponse(
                    name=sq.get("name"),
                    label=sq.get("label"),
                    description=sq.get("description"),
                    metrics=[
                        m.get("name")
                        for m in (sq.get("queryParams") or {}).get("metrics", [])
                    ]
                    if (sq.get("queryParams") or {}).get("metrics")
                    else None,
                    group_by=[
                        g.get("name")
                        for g in (sq.get("queryParams") or {}).get("groupBy", [])
                    ]
                    if (sq.get("queryParams") or {}).get("groupBy")
                    else None,
                    where=(sq.get("queryParams") or {})
                    .get("where", {})
                    .get("whereSqlTemplate")
                    if (sq.get("queryParams") or {}).get("where")
                    else None,
                )
                for sq in full_result["data"]["savedQueriesPaginated"]["items"]
            ]
        except Exception as e:
            logger.warning(f"Error fetching saved queries with params: {e}")
            results = simple_results
        search = search.strip() if isinstance(search, str) else search
        search = search if search else None
        if search:
            search_lower = search.lower()
            results = [
                r
                for r in results
                if search_lower in (r.name or "").lower()
                or search_lower in (r.label or "").lower()
                or search_lower in (r.description or "").lower()
            ]
        return ResultPage(
            result=results,
            pagination=numbered_pagination(
                saved_queries_result["data"]["savedQueriesPaginated"]
            ),
        )

    async def get_dimensions(
        self,
        config: SemanticLayerConfig,
        metrics: list[str],
        search: str | None = None,
        *,
        page_num: int = 1,
        page_size: int = 50,
    ) -> ResultPage[list[DimensionToolResponse]]:
        validate_page_size(page_size)
        validate_page_number(page_num)
        dimensions_result = await submit_request(
            config,
            {
                "query": GRAPHQL_QUERIES["dimensions"],
                "variables": {
                    "metrics": [{"name": m} for m in metrics],
                    "search": search,
                    "pageNum": page_num,
                    "pageSize": page_size,
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
        return ResultPage(
            result=dimensions,
            pagination=numbered_pagination(
                dimensions_result["data"]["dimensionsPaginated"]
            ),
        )

    async def get_entities(
        self,
        config: SemanticLayerConfig,
        metrics: list[str],
        search: str | None = None,
        *,
        page_num: int = 1,
        page_size: int = 50,
    ) -> ResultPage[list[EntityToolResponse]]:
        validate_page_size(page_size)
        validate_page_number(page_num)
        entities_result = await submit_request(
            config,
            {
                "query": GRAPHQL_QUERIES["entities"],
                "variables": {
                    "metrics": [{"name": m} for m in metrics],
                    "search": search,
                    "pageNum": page_num,
                    "pageSize": page_size,
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
        return ResultPage(
            result=entities,
            pagination=numbered_pagination(
                entities_result["data"]["entitiesPaginated"]
            ),
        )

    async def get_dimension_values(
        self,
        config: SemanticLayerConfig,
        dimension: str,
        metrics: list[str] | None = None,
        limit: int = 100,
    ) -> DimensionValuesResult:
        try:
            sl_client = await self.client_provider.get_client(config=config)

            # Opening a session is blocking I/O (it establishes a GraphQL and an
            # ADBC/Arrow-Flight connection), so run the whole session lifecycle —
            # open, query, close — in a worker thread to keep it off the event loop.
            def query() -> pa.Table:
                with sl_client.session():
                    return sl_client.dimension_values(
                        metrics=metrics or [],
                        group_by=dimension,
                    )

            raw_table: pa.Table = await asyncio.to_thread(query)
            # SDK doesn't support server-side limiting; truncation is applied client-side.
            # SDK returns column names in uppercase; match case-insensitively.
            schema_names = raw_table.schema.names
            column_name = next(
                (n for n in schema_names if n.lower() == dimension.lower()),
                None,
            )
            if column_name is None:
                return DimensionValuesError(
                    error=f"Dimension '{dimension}' not found in result schema. "
                    f"Available columns: {schema_names}"
                )
            raw: list[str] = [
                str(v)
                for v in raw_table.column(column_name).to_pylist()
                if v is not None
            ]
            truncated = len(raw) > limit
            return DimensionValuesResponse(values=raw[:limit], truncated=truncated)
        except QueryFailedError as e:
            return DimensionValuesError(
                error=await self._format_error_with_hint(e, config)
            )

    def _format_semantic_layer_error(self, error: Exception) -> str:
        """Format semantic layer errors by cleaning up common error message patterns."""
        # QueryFailedError.__str__ wraps its message in a `message="...", status=...`
        # artifact of that class's __str__ implementation. Use the clean `.message`
        # attribute instead, so the cleanup chain below operates on the real
        # underlying message rather than that wrapper.
        if isinstance(error, QueryFailedError):
            error_str = str(error.message) if error.message is not None else ""
        else:
            error_str = str(error)

        # The semantic layer may prefix a warehouse query failure's message with
        # a bracketed classification marker. Strip it out before the cosmetic
        # cleanup below, since that cleanup's `.lstrip("[")` would otherwise
        # corrupt an unstripped marker.
        error_str, warehouse_error_category = classify_warehouse_error(error_str)

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
            formatted = (
                error_str or f"Semantic layer query failed: {type(error).__name__}"
            )

        hint = warehouse_error_hint(warehouse_error_category)
        if hint:
            formatted = f"{formatted}\n\n{hint}"
        return formatted

    async def _format_error_with_hint(
        self, error: Exception, config: SemanticLayerConfig
    ) -> str:
        """Format the error and, if the warehouse auth expired, say how to fix it."""
        formatted = self._format_semantic_layer_error(error)
        hint_provider = config.warehouse_auth_hint_provider
        # Only query failures come from the warehouse; other exceptions (e.g. an
        # expired dbt platform login) are not fixed by reconnecting the warehouse.
        if (
            hint_provider is None
            or not isinstance(error, QueryFailedError)
            or not is_warehouse_auth_error(formatted)
        ):
            return formatted
        try:
            hint = await hint_provider.get_hint(
                environment_id=config.prod_environment_id
            )
        except Exception:
            logger.warning("Could not build the warehouse auth hint", exc_info=True)
            return formatted
        return append_hint(formatted, hint)

    async def _format_get_metrics_compiled_sql_error(
        self, compile_error: Exception, config: SemanticLayerConfig
    ) -> GetMetricsCompiledSqlError:
        """Format get compiled SQL errors using the shared error formatter."""
        return GetMetricsCompiledSqlError(
            error=await self._format_error_with_hint(compile_error, config)
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
    async def _format_query_failed_error(
        self, query_error: Exception, config: SemanticLayerConfig
    ) -> QueryMetricsError:
        if isinstance(query_error, QueryFailedError):
            return QueryMetricsError(
                error=await self._format_error_with_hint(query_error, config)
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
        group_by_map = {g.name: g for g in group_by} if group_by else {}
        queried_metrics = set(metrics)
        for o in order_by:
            if o.name in queried_metrics:
                result.append(OrderByMetric(name=o.name, descending=o.descending))
            elif o.name in group_by_map:
                result.append(
                    OrderByGroupBy(
                        name=o.name,
                        descending=o.descending,
                        grain=o.grain
                        if o.grain is not None
                        else group_by_map[o.name].grain,
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
            sl_client = await self.client_provider.get_client(config=config)
            parsed_order_by: list[OrderBySpec] = self._get_order_bys(
                order_by=order_by, metrics=metrics, group_by=group_by
            )
            normalized_where = self._normalize_where(where)

            # Run the whole session lifecycle off the event loop — see the note
            # in get_dimension_values; opening a session is blocking I/O.
            def query() -> str:
                with sl_client.session():
                    return sl_client.compile_sql(
                        metrics=metrics,
                        group_by=group_by,  # type: ignore
                        order_by=parsed_order_by,  # type: ignore
                        where=[normalized_where] if normalized_where else None,
                        limit=limit,
                        read_cache=True,
                    )

            compiled_sql = await asyncio.to_thread(query)
            return GetMetricsCompiledSqlSuccess(sql=compiled_sql)

        except Exception as e:
            return await self._format_get_metrics_compiled_sql_error(e, config)

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
            # Only `query_metrics` has a GraphQL fallback to recover a call that
            # times out, so only its Flight SQL client has a time limit.
            sl_client = await self.client_provider.get_client(
                config=config, time_bounded=True
            )
            parsed_order_by: list[OrderBySpec] = self._get_order_bys(
                order_by=order_by, metrics=metrics, group_by=group_by
            )
            normalized_where = self._normalize_where(where)

            # Run the whole session lifecycle off the event loop — see the note
            # in get_dimension_values; opening a session is blocking I/O.
            def run(client: SemanticLayerQueryClientProtocol) -> pa.Table:
                with client.session():
                    return client.query(
                        metrics=metrics,
                        group_by=group_by,  # type: ignore
                        order_by=parsed_order_by,  # type: ignore
                        where=[normalized_where] if normalized_where else None,
                        limit=limit,
                    )

            # Flight SQL is the primary path. When its connection fails, the same
            # query runs over the GraphQL API, which reaches the Semantic Layer
            # over a separate protocol. Every other error, including a query the
            # Semantic Layer rejects, propagates as before.
            async def run_with_graphql_fallback() -> pa.Table:
                try:
                    return await asyncio.to_thread(run, sl_client)
                except OperationalError as e:
                    if e.status_code not in _CONNECTION_FAILURE_STATUS_CODES:
                        raise
                    logger.warning(
                        "Flight SQL connection failed; retrying query over GraphQL: %s",
                        e,
                    )
                    graphql_client = await self.client_provider.get_graphql_client(
                        config=config
                    )
                    return await asyncio.to_thread(run, graphql_client)

            # Only query-level failures (the SL processed the query and rejected
            # it) are returned to the caller as data. Operational failures (auth,
            # transport, or a connection that fails over both protocols) propagate
            # so callers can distinguish a bad query from an unreachable semantic
            # layer.
            try:
                query_result = await run_with_graphql_fallback()
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
            except QueryFailedError as e:
                query_error = e
            if query_error:
                return await self._format_query_failed_error(query_error, config)
            formatter = result_formatter or DEFAULT_RESULT_FORMATTER
            json_result = await asyncio.to_thread(formatter, query_result)
            return QueryMetricsSuccess(result=json_result or "")
        except SemanticLayerQueryTimeoutError:
            raise
        except QueryFailedError as e:
            return await self._format_query_failed_error(e, config)
