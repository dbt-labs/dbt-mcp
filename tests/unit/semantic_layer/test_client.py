import datetime as dt

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
