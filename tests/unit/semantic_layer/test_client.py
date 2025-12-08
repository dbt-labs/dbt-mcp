import base64
import datetime as dt
import json
from decimal import Decimal

import pyarrow as pa

from dbt_mcp.semantic_layer.client import DEFAULT_RESULT_FORMATTER


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
