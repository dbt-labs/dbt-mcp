import base64
import datetime as dt
import json
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest
from dbtsl.error import RetryTimeoutError

from dbtsl.api.shared.query_params import (
    GroupByParam,
    GroupByType,
    OrderByGroupBy,
    OrderByMetric,
)

from dbt_mcp.config.config_providers import SemanticLayerConfig
from dbt_mcp.errors import InvalidParameterError
from dbt_mcp.errors.semantic_layer import SemanticLayerQueryTimeoutError
from dbt_mcp.semantic_layer.client import DEFAULT_RESULT_FORMATTER, SemanticLayerFetcher
from dbt_mcp.semantic_layer.types import OrderByParam, QueryMetricsError


def test_default_result_formatter_outputs_iso_dates() -> None:
    timestamp = dt.datetime(2025, 9, 1, tzinfo=dt.UTC)
    table = pa.table(
        {
            "METRIC_TIME__MONTH": pa.array(
                [timestamp],
                type=pa.timestamp("ms", tz="UTC"),
            ),
            "MRR": pa.array([1234.56]),
        }
    )
    output = DEFAULT_RESULT_FORMATTER(table)
    assert "2025-09-01T00:00:00" in output
    assert "1756684800000" not in output


def test_default_result_formatter_returns_valid_json() -> None:
    """Test that the output is valid JSON that can be parsed."""
    table = pa.table(
        {
            "metric": pa.array([100, 200, 300]),
            "dimension": pa.array(["a", "b", "c"]),
        }
    )
    output = DEFAULT_RESULT_FORMATTER(table)

    # Should be valid JSON
    parsed = json.loads(output)
    assert isinstance(parsed, list)
    assert len(parsed) == 3


def test_default_result_formatter_records_format() -> None:
    """Test that output is in records format (array of objects)."""
    table = pa.table(
        {
            "revenue": pa.array([100.5, 200.75]),
            "region": pa.array(["North", "South"]),
        }
    )
    output = DEFAULT_RESULT_FORMATTER(table)
    parsed = json.loads(output)

    # Should be array of dicts
    assert isinstance(parsed, list)
    assert len(parsed) == 2

    # First record
    assert parsed[0]["revenue"] == 100.5
    assert parsed[0]["region"] == "North"

    # Second record
    assert parsed[1]["revenue"] == 200.75
    assert parsed[1]["region"] == "South"


def test_default_result_formatter_indentation() -> None:
    """Test that output uses proper indentation (indent=2)."""
    table = pa.table(
        {
            "metric": pa.array([100]),
            "name": pa.array(["test"]),
        }
    )
    output = DEFAULT_RESULT_FORMATTER(table)

    # Check for indentation in output
    assert "  " in output  # Should have 2-space indentation
    # Verify it's properly formatted (not all on one line)
    assert "\n" in output


def test_default_result_formatter_with_nulls() -> None:
    """Test handling of null values."""
    table = pa.table(
        {
            "value": pa.array([100, None, 300]),
            "name": pa.array(["a", "b", None]),
        }
    )
    output = DEFAULT_RESULT_FORMATTER(table)
    parsed = json.loads(output)

    assert parsed[1]["value"] is None
    assert parsed[2]["name"] is None


def test_default_result_formatter_with_dates() -> None:
    """Test handling of date objects (not just timestamps)."""
    date_val = dt.date(2024, 1, 15)
    table = pa.table(
        {
            "date_col": pa.array([date_val], type=pa.date32()),
            "value": pa.array([42]),
        }
    )
    output = DEFAULT_RESULT_FORMATTER(table)
    parsed = json.loads(output)

    # Should contain ISO formatted date
    assert "2024-01-15" in output
    assert isinstance(parsed[0], dict)


def test_default_result_formatter_empty_table() -> None:
    """Test handling of empty tables."""
    table = pa.table(
        {
            "metric": pa.array([], type=pa.int64()),
            "name": pa.array([], type=pa.string()),
        }
    )
    output = DEFAULT_RESULT_FORMATTER(table)
    parsed = json.loads(output)

    assert isinstance(parsed, list)
    assert len(parsed) == 0


def test_default_result_formatter_various_numeric_types() -> None:
    """Test handling of different numeric types."""
    table = pa.table(
        {
            "int_col": pa.array([1, 2, 3]),
            "float_col": pa.array([1.1, 2.2, 3.3]),
            "decimal_col": pa.array([100.50, 200.75, 300.25]),
        }
    )
    output = DEFAULT_RESULT_FORMATTER(table)
    parsed = json.loads(output)

    assert parsed[0]["int_col"] == 1
    assert abs(parsed[0]["float_col"] - 1.1) < 0.0001
    assert abs(parsed[0]["decimal_col"] - 100.50) < 0.0001


def test_default_result_formatter_with_python_decimal() -> None:
    """Test handling of Python Decimal objects from PyArrow decimal128 columns.

    This tests the fix for Decimal JSON serialization where PyArrow decimal128
    columns return Python Decimal objects that need special handling in JSON encoding.
    """
    # Create a PyArrow table with decimal128 type (which returns Python Decimal objects)
    decimal_array = pa.array(
        [Decimal("123.45"), Decimal("678.90"), Decimal("0.01")],
        type=pa.decimal128(10, 2),
    )
    table = pa.table(
        {
            "amount": decimal_array,
            "name": pa.array(["a", "b", "c"]),
        }
    )

    # This should not raise "Object of type Decimal is not JSON serializable"
    output = DEFAULT_RESULT_FORMATTER(table)
    parsed = json.loads(output)

    # Verify Decimal values are correctly converted to floats
    assert abs(parsed[0]["amount"] - 123.45) < 0.0001
    assert abs(parsed[1]["amount"] - 678.90) < 0.0001
    assert abs(parsed[2]["amount"] - 0.01) < 0.0001


def test_default_result_formatter_with_time_objects() -> None:
    """Test handling of Python time objects from PyArrow time64 columns.

    PyArrow time columns return Python time objects that need special handling
    in JSON encoding.
    """
    # Create a PyArrow table with time64 type (which returns Python time objects)
    # Time value in microseconds: 3661000000 = 01:01:01
    time_array = pa.array([3661000000, 0, 86399999999], type=pa.time64("us"))
    table = pa.table(
        {
            "time_col": time_array,
            "id": pa.array([1, 2, 3]),
        }
    )

    # This should not raise "Object of type time is not JSON serializable"
    output = DEFAULT_RESULT_FORMATTER(table)
    parsed = json.loads(output)

    # Verify time values are correctly converted to ISO format strings
    assert parsed[0]["time_col"] == "01:01:01"
    assert parsed[1]["time_col"] == "00:00:00"
    assert "23:59:59" in parsed[2]["time_col"]


def test_default_result_formatter_with_timedelta_objects() -> None:
    """Test handling of Python timedelta objects from PyArrow duration columns.

    PyArrow duration columns return Python timedelta objects that need special
    handling in JSON encoding.
    """
    # Create a PyArrow table with duration type (which returns Python timedelta objects)
    # Duration in microseconds: 3661000000 = 1 hour, 1 minute, 1 second
    duration_array = pa.array([3661000000, 1000000, 0], type=pa.duration("us"))
    table = pa.table(
        {
            "duration_col": duration_array,
            "name": pa.array(["long", "short", "zero"]),
        }
    )

    # This should not raise "Object of type timedelta is not JSON serializable"
    output = DEFAULT_RESULT_FORMATTER(table)
    parsed = json.loads(output)

    # Verify timedelta values are correctly converted to total seconds (float)
    assert parsed[0]["duration_col"] == 3661.0  # 1 hour + 1 minute + 1 second
    assert parsed[1]["duration_col"] == 1.0  # 1 second
    assert parsed[2]["duration_col"] == 0.0  # 0 seconds


def test_default_result_formatter_with_binary_objects() -> None:
    """Test handling of Python bytes objects from PyArrow binary columns.

    PyArrow binary columns return Python bytes objects that need special handling
    in JSON encoding. They are encoded as base64 strings.
    """
    # Create a PyArrow table with binary type (which returns Python bytes objects)
    binary_array = pa.array([b"hello", b"world", b"\x00\x01\x02"], type=pa.binary())
    table = pa.table(
        {
            "binary_col": binary_array,
            "id": pa.array([1, 2, 3]),
        }
    )

    # This should not raise "Object of type bytes is not JSON serializable"
    output = DEFAULT_RESULT_FORMATTER(table)
    parsed = json.loads(output)

    # Verify bytes values are correctly converted to base64 encoded strings
    assert parsed[0]["binary_col"] == base64.b64encode(b"hello").decode("utf-8")
    assert parsed[1]["binary_col"] == base64.b64encode(b"world").decode("utf-8")
    assert parsed[2]["binary_col"] == base64.b64encode(b"\x00\x01\x02").decode("utf-8")


def test_default_result_formatter_with_mixed_types() -> None:
    """Test handling of a table with multiple special types together."""
    # Create a table with datetime, date, time, decimal, timedelta, and binary
    table = pa.table(
        {
            "timestamp_col": pa.array(
                [dt.datetime(2025, 1, 1, 12, 30, tzinfo=dt.UTC)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "date_col": pa.array([dt.date(2025, 1, 1)], type=pa.date32()),
            "time_col": pa.array([43200000000], type=pa.time64("us")),  # 12:00:00
            "decimal_col": pa.array([Decimal("99.99")], type=pa.decimal128(10, 2)),
            "duration_col": pa.array([7200000000], type=pa.duration("us")),  # 2 hours
            "binary_col": pa.array([b"data"], type=pa.binary()),
        }
    )

    # Should handle all types without errors
    output = DEFAULT_RESULT_FORMATTER(table)
    parsed = json.loads(output)

    # Verify all types are properly serialized
    assert "2025-01-01" in parsed[0]["timestamp_col"]
    assert parsed[0]["date_col"] == "2025-01-01"
    assert parsed[0]["time_col"] == "12:00:00"
    assert abs(parsed[0]["decimal_col"] - 99.99) < 0.0001
    assert parsed[0]["duration_col"] == 7200.0
    assert parsed[0]["binary_col"] == base64.b64encode(b"data").decode("utf-8")


MOCK_METRICS_RESPONSE = {
    "data": {
        "metricsPaginated": {
            "items": [
                {
                    "name": "revenue",
                    "type": "simple",
                    "label": "Revenue",
                    "description": "Total revenue",
                    "config": None,
                }
            ]
        }
    }
}

MOCK_METRICS_WITH_RELATED_RESPONSE = {
    "data": {
        "metricsPaginated": {
            "items": [
                {
                    "name": "revenue",
                    "type": "simple",
                    "label": "Revenue",
                    "description": "Total revenue",
                    "config": None,
                    "dimensions": [
                        {
                            "name": "order_date",
                            "type": "time",
                            "description": None,
                            "label": None,
                            "queryableGranularities": ["day"],
                            "queryableTimeGranularities": [],
                            "config": None,
                        }
                    ],
                    "entities": [
                        {
                            "name": "customer",
                            "type": "primary",
                            "description": None,
                        }
                    ],
                }
            ]
        }
    }
}


def _make_query_dispatcher():
    """Return a side_effect function that dispatches mock responses by query content.

    Distinguishes the lightweight metrics query from the full metrics_with_related
    query by checking for the presence of nested 'dimensions {' in the query string.
    """

    def dispatch(_, payload, **kwargs):
        query = payload.get("query", "")
        if "metricsPaginated" in query:
            if "dimensions {" in query:
                return MOCK_METRICS_WITH_RELATED_RESPONSE
            return MOCK_METRICS_RESPONSE
        if "dimensionsPaginated" in query:
            return {"data": {"dimensionsPaginated": {"items": []}}}
        if "entitiesPaginated" in query:
            return {"data": {"entitiesPaginated": {"items": []}}}
        raise AssertionError(f"Unexpected GraphQL query: {query}")

    return dispatch


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
async def test_list_metrics_below_threshold_returns_full_config(
    mock_submit_request, fetcher, mock_config_provider
):
    """When metric count <= threshold, dims and entities are embedded per metric."""
    mock_submit_request.side_effect = _make_query_dispatcher()
    config = mock_config_provider.get_config.return_value
    result = await fetcher.list_metrics(config=config)

    assert len(result.metrics) == 1
    metric = result.metrics[0]
    assert metric.dimensions == ["order_date"]
    assert metric.entities == ["customer"]
    assert mock_submit_request.call_count == 2


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
async def test_list_metrics_above_threshold_returns_metrics_only(
    mock_submit_request, mock_client_provider, mock_config_provider
):
    """When metric count > threshold, only metrics are returned (no dims/entities)."""
    many_metrics = [
        {
            "name": f"metric_{i}",
            "type": "simple",
            "label": None,
            "description": None,
            "config": None,
        }
        for i in range(3)
    ]
    mock_submit_request.return_value = {
        "data": {"metricsPaginated": {"items": many_metrics}}
    }
    config = mock_config_provider.get_config.return_value
    config.metrics_related_max = 2
    fetcher = SemanticLayerFetcher(client_provider=mock_client_provider)
    result = await fetcher.list_metrics(config=config)

    assert len(result.metrics) == 3
    assert all(m.dimensions is None for m in result.metrics)
    assert all(m.entities is None for m in result.metrics)
    assert mock_submit_request.call_count == 1


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
async def test_list_metrics_at_threshold_returns_full_config(
    mock_submit_request, mock_client_provider, mock_config_provider
):
    """When metric count == threshold, dims and entities are embedded per metric."""
    mock_submit_request.side_effect = _make_query_dispatcher()
    config = mock_config_provider.get_config.return_value
    config.metrics_related_max = 1
    fetcher = SemanticLayerFetcher(client_provider=mock_client_provider)
    result = await fetcher.list_metrics(config=config)

    assert result.metrics[0].dimensions is not None
    assert result.metrics[0].entities is not None


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
async def test_list_metrics_search_list_fans_out_and_dedupes(
    mock_submit_request, mock_client_provider, mock_config_provider
):
    """A list of search terms triggers one GraphQL call per term, then dedupes."""
    revenue_item = {
        "name": "revenue",
        "type": "simple",
        "label": "Revenue",
        "description": None,
        "config": None,
    }
    cost_item = {
        "name": "cost",
        "type": "simple",
        "label": "Cost",
        "description": None,
        "config": None,
    }

    def dispatch(_, payload, **kwargs):
        # Threshold is exceeded so only the cheap query fires; one call per term.
        search = payload["variables"].get("search")
        if search == "rev":
            # Returns revenue plus the duplicate that the other term also matches.
            return {"data": {"metricsPaginated": {"items": [revenue_item, cost_item]}}}
        if search == "cost":
            return {"data": {"metricsPaginated": {"items": [cost_item]}}}
        raise AssertionError(f"Unexpected search term: {search!r}")

    mock_submit_request.side_effect = dispatch
    config = mock_config_provider.get_config.return_value
    config.metrics_related_max = 1  # force the "cheap only" branch
    fetcher = SemanticLayerFetcher(client_provider=mock_client_provider)

    result = await fetcher.list_metrics(config=config, search=["rev", "cost"])

    # One call per search term — fan-out.
    assert mock_submit_request.call_count == 2
    # Merged and deduped, order preserved by first-seen.
    assert [m.name for m in result.metrics] == ["revenue", "cost"]


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
async def test_list_metrics_search_list_below_threshold_fans_out_related(
    mock_submit_request, mock_client_provider, mock_config_provider
):
    """When the deduped result is small enough, the with_related query also fans out."""

    def dispatch(_, payload, **kwargs):
        query = payload["query"]
        # Both the cheap and the with_related queries get one call per term.
        if "dimensions {" in query:
            return MOCK_METRICS_WITH_RELATED_RESPONSE
        if "metricsPaginated" in query:
            return MOCK_METRICS_RESPONSE
        raise AssertionError(f"Unexpected query: {query}")

    mock_submit_request.side_effect = dispatch
    config = mock_config_provider.get_config.return_value
    fetcher = SemanticLayerFetcher(client_provider=mock_client_provider)

    result = await fetcher.list_metrics(config=config, search=["a", "b"])

    # 2 cheap + 2 with_related = 4 calls
    assert mock_submit_request.call_count == 4
    # Deduped to a single metric with related fields populated
    assert len(result.metrics) == 1
    assert result.metrics[0].dimensions == ["order_date"]
    assert result.metrics[0].entities == ["customer"]


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
async def test_list_metrics_search_list_normalizes_and_dedupes_terms(
    mock_submit_request, fetcher, mock_config_provider
):
    """Whitespace-only terms are dropped; identical terms are deduped before fan-out."""
    mock_submit_request.side_effect = _make_query_dispatcher()
    config = mock_config_provider.get_config.return_value

    await fetcher.list_metrics(
        config=config,
        # "rev" appears twice (once whitespace-padded), "  " is empty after strip,
        # so the dedupe should collapse this to a single search term "rev".
        search=["rev", "  rev  ", "  ", "rev"],
    )

    # 1 cheap + 1 with_related = 2 calls total, never broadened to no-filter.
    assert mock_submit_request.call_count == 2
    for call in mock_submit_request.call_args_list:
        assert call.args[1]["variables"]["search"] == "rev"


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
async def test_list_metrics_single_string_search_is_normalized(
    mock_submit_request, fetcher, mock_config_provider
):
    """A single-string search is stripped and empty/whitespace-only becomes no filter."""
    mock_submit_request.side_effect = _make_query_dispatcher()
    config = mock_config_provider.get_config.return_value

    # Whitespace-padded string is stripped to "rev"
    await fetcher.list_metrics(config=config, search="  rev  ")
    for call in mock_submit_request.call_args_list:
        assert call.args[1]["variables"]["search"] == "rev"
    mock_submit_request.reset_mock()
    mock_submit_request.side_effect = _make_query_dispatcher()

    # Empty/whitespace-only string collapses to no filter
    await fetcher.list_metrics(config=config, search="   ")
    for call in mock_submit_request.call_args_list:
        assert call.args[1]["variables"]["search"] is None


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
async def test_list_metrics_search_list_caps_term_count(
    mock_submit_request, fetcher, mock_config_provider
):
    """An overly long search list raises rather than firing unbounded parallel calls."""
    from dbt_mcp.errors import InvalidParameterError
    from dbt_mcp.semantic_layer.client import _MAX_SEARCH_TERMS

    config = mock_config_provider.get_config.return_value
    too_many = [f"term_{i}" for i in range(_MAX_SEARCH_TERMS + 1)]

    with pytest.raises(InvalidParameterError, match=str(_MAX_SEARCH_TERMS)):
        await fetcher.list_metrics(config=config, search=too_many)
    # No GraphQL request was issued
    mock_submit_request.assert_not_called()


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
async def test_list_metrics_empty_search_list_treated_as_no_filter(
    mock_submit_request, fetcher, mock_config_provider
):
    """An empty list (or list of empty strings) behaves like search=None."""
    mock_submit_request.side_effect = _make_query_dispatcher()
    config = mock_config_provider.get_config.return_value

    result = await fetcher.list_metrics(config=config, search=[])

    # Single fan-out term (None), so cheap + related = 2 calls
    assert mock_submit_request.call_count == 2
    assert len(result.metrics) == 1
    # The single GraphQL call received search=None
    for call in mock_submit_request.call_args_list:
        assert call.args[1]["variables"]["search"] is None


@pytest.mark.parametrize(
    "input_where,expected",
    [
        (None, None),
        ("", None),
        ("   ", None),
        ("metric_time > '2024-01-01'", "metric_time > '2024-01-01'"),
        ("\"metric_time > '2024-01-01'\"", "metric_time > '2024-01-01'"),
        ("  \"metric_time > '2024-01-01'\"  ", "metric_time > '2024-01-01'"),
        ('"   "', None),
    ],
)
def test_normalize_where(fetcher, input_where, expected) -> None:
    assert fetcher._normalize_where(input_where) == expected


def test_format_semantic_layer_error_cleans_query_failed_error(fetcher) -> None:
    """Normal QueryFailedError messages should be cleaned up."""
    error = Exception(
        "QueryFailedError(INVALID_ARGUMENT: [FlightSQL] Failed to prepare statement: "
        "com.dbt.semanticlayer.exceptions.DataPlatformException: column not found"
    )
    result = fetcher._format_semantic_layer_error(error)
    assert result == "column not found"


def test_format_semantic_layer_error_fallback_on_empty(fetcher) -> None:
    """When cleaning strips the message to empty, fall back to the original."""
    error = Exception("[]")
    result = fetcher._format_semantic_layer_error(error)
    assert result == "[]"


def test_format_semantic_layer_error_fallback_on_empty_str(fetcher) -> None:
    """When the original str is also empty, fall back to the class name."""
    error = RuntimeError()
    result = fetcher._format_semantic_layer_error(error)
    assert "RuntimeError" in result


@pytest.fixture
def mock_config_provider():
    config_provider = AsyncMock()
    config_provider.get_config.return_value = MagicMock(
        prod_environment_id=123,
        token="test-token",
        host="test-host",
        url="https://test-host/api/graphql",
        metrics_related_max=10,
    )
    return config_provider


@pytest.fixture
def mock_client_provider():
    return AsyncMock()


@pytest.fixture
def fetcher(mock_client_provider):
    return SemanticLayerFetcher(
        client_provider=mock_client_provider,
    )


@pytest.mark.asyncio
@patch("dbt_mcp.semantic_layer.client.submit_request")
async def test_get_dimensions_includes_metadata(
    mock_submit_request, fetcher, mock_config_provider
):
    mock_submit_request.return_value = {
        "data": {
            "dimensionsPaginated": {
                "items": [
                    {
                        "name": "order_date",
                        "type": "time",
                        "description": "Order timestamp",
                        "label": "Order Date",
                        "queryableGranularities": ["day"],
                        "queryableTimeGranularities": ["month"],
                        "config": {"meta": {"display_name": "Order Date"}},
                    },
                    {
                        "name": "customer_type",
                        "type": "categorical",
                        "description": "Customer segment",
                        "label": None,
                        "queryableGranularities": [],
                        "queryableTimeGranularities": [],
                        "config": None,
                    },
                    {
                        "name": "order_status",
                        "type": "categorical",
                        "description": "Order status",
                        "label": "Status",
                        "queryableGranularities": [],
                        "queryableTimeGranularities": [],
                    },
                ]
            }
        }
    }

    result = await fetcher.get_dimensions(
        config=mock_config_provider.get_config.return_value, metrics=["revenue"]
    )

    assert len(result) == 3
    assert result[0].metadata == {"display_name": "Order Date"}
    assert result[1].metadata is None
    assert result[2].metadata is None


class TestQueryMetricsCompiledTimeout:
    """Tests for COMPILED timeout handling in query_metrics."""

    @pytest.fixture
    def mock_sl_client(self):
        client = MagicMock()
        session_ctx = MagicMock()
        client.session.return_value = session_ctx
        session_ctx.__enter__ = MagicMock(return_value=client)
        session_ctx.__exit__ = MagicMock(return_value=False)
        return client

    @pytest.fixture
    def mock_config(self):
        token_p = MagicMock()
        token_p.get_token.return_value = "tok"
        headers_p = MagicMock()
        headers_p.get_headers.return_value = {}
        return SemanticLayerConfig(
            url="https://test-host/api/graphql",
            host="test-host",
            prod_environment_id=123,
            token_provider=token_p,
            headers_provider=headers_p,
        )

    @pytest.fixture
    def compiled_fetcher(self, mock_sl_client):
        client_provider = AsyncMock()
        client_provider.get_client.return_value = mock_sl_client
        return SemanticLayerFetcher(
            client_provider=client_provider,
        )

    async def test_compiled_timeout_raises_client_error(
        self, compiled_fetcher, mock_sl_client, mock_config
    ):
        """COMPILED status timeout should raise SemanticLayerQueryTimeoutError."""
        mock_sl_client.query.side_effect = RetryTimeoutError(
            timeout_s=60, status="COMPILED"
        )

        with pytest.raises(SemanticLayerQueryTimeoutError) as exc_info:
            await compiled_fetcher.query_metrics(
                config=mock_config, metrics=["revenue"]
            )

        assert "COMPILED" in str(exc_info.value)

    async def test_running_timeout_returns_error_result(
        self, compiled_fetcher, mock_sl_client, mock_config
    ):
        """RUNNING status timeout should return QueryMetricsError, not raise."""
        mock_sl_client.query.side_effect = RetryTimeoutError(
            timeout_s=60, status="RUNNING"
        )

        result = await compiled_fetcher.query_metrics(
            config=mock_config, metrics=["revenue"]
        )

        assert isinstance(result, QueryMetricsError)
        assert result.error is not None

    async def test_none_status_timeout_returns_error_result(
        self, compiled_fetcher, mock_sl_client, mock_config
    ):
        """None status timeout should return QueryMetricsError, not raise."""
        mock_sl_client.query.side_effect = RetryTimeoutError(timeout_s=60)

        result = await compiled_fetcher.query_metrics(
            config=mock_config, metrics=["revenue"]
        )

        assert isinstance(result, QueryMetricsError)
        assert result.error is not None


@pytest.mark.asyncio
async def test_query_metrics_sdk_client_uses_fetcher_config_override(
    mock_client_provider,
):
    token_p = MagicMock()
    token_p.get_token.return_value = "tok"
    headers_p = MagicMock()
    headers_p.get_headers.return_value = {}
    override = SemanticLayerConfig(
        url="https://example.com/graphql",
        host="example.com",
        prod_environment_id=777,
        token_provider=token_p,
        headers_provider=headers_p,
    )
    mock_sl_client = MagicMock()
    session_ctx = MagicMock()
    mock_sl_client.session.return_value = session_ctx
    session_ctx.__enter__ = MagicMock(return_value=mock_sl_client)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_sl_client.query.return_value = pa.table({"a": [1]})
    mock_client_provider.get_client.return_value = mock_sl_client

    fetcher = SemanticLayerFetcher(
        client_provider=mock_client_provider,
    )
    await fetcher.query_metrics(config=override, metrics=["revenue"])

    mock_client_provider.get_client.assert_awaited_once_with(config=override)


@pytest.mark.asyncio
async def test_get_metrics_compiled_sql_sdk_client_uses_fetcher_config_override(
    mock_client_provider,
):
    token_p = MagicMock()
    token_p.get_token.return_value = "tok"
    headers_p = MagicMock()
    headers_p.get_headers.return_value = {}
    override = SemanticLayerConfig(
        url="https://example.com/graphql",
        host="example.com",
        prod_environment_id=888,
        token_provider=token_p,
        headers_provider=headers_p,
    )
    mock_sl_client = MagicMock()
    session_ctx = MagicMock()
    mock_sl_client.session.return_value = session_ctx
    session_ctx.__enter__ = MagicMock(return_value=mock_sl_client)
    session_ctx.__exit__ = MagicMock(return_value=False)
    mock_sl_client.compile_sql.return_value = "select 1"
    mock_client_provider.get_client.return_value = mock_sl_client

    fetcher = SemanticLayerFetcher(
        client_provider=mock_client_provider,
    )
    await fetcher.get_metrics_compiled_sql(config=override, metrics=["revenue"])

    mock_client_provider.get_client.assert_awaited_once_with(config=override)


# Tests for _get_order_bys


def test_get_order_bys_explicit_grain_takes_precedence(fetcher) -> None:
    group_by = [
        GroupByParam(name="metric_time", type=GroupByType.TIME_DIMENSION, grain="MONTH")
    ]
    order_by = [OrderByParam(name="metric_time", descending=True, grain="WEEK")]
    result = fetcher._get_order_bys(order_by, group_by=group_by)
    assert result == [OrderByGroupBy(name="metric_time", descending=True, grain="WEEK")]


def test_get_order_bys_falls_back_to_group_by_grain(fetcher) -> None:
    group_by = [
        GroupByParam(name="metric_time", type=GroupByType.TIME_DIMENSION, grain="MONTH")
    ]
    order_by = [OrderByParam(name="metric_time", descending=False)]
    result = fetcher._get_order_bys(order_by, group_by=group_by)
    assert result == [
        OrderByGroupBy(name="metric_time", descending=False, grain="MONTH")
    ]


def test_get_order_bys_metric_name(fetcher) -> None:
    order_by = [OrderByParam(name="revenue", descending=True)]
    result = fetcher._get_order_bys(order_by, metrics=["revenue"])
    assert result == [OrderByMetric(name="revenue", descending=True)]


def test_get_order_bys_unknown_name_raises(fetcher) -> None:
    order_by = [OrderByParam(name="unknown", descending=False)]
    with pytest.raises(InvalidParameterError):
        fetcher._get_order_bys(order_by, metrics=["revenue"], group_by=[])


def test_get_order_bys_none_returns_empty(fetcher) -> None:
    assert fetcher._get_order_bys(None) == []
