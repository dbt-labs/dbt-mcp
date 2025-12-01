import datetime as dt
import json

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
