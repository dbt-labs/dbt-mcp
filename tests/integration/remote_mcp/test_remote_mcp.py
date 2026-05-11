import csv
import io

from dbt_mcp.config.config import load_config
from dbt_mcp.mcp.server import create_dbt_mcp
from remote_mcp.session import session_context


async def test_local_mcp_list_metrics_returns_valid_response() -> None:
    config = load_config()
    dbt_mcp = await create_dbt_mcp(config)
    result = await dbt_mcp.call_tool(
        name="list_metrics",
        arguments={},
    )
    assert isinstance(result, list)
    assert len(result) == 1
    content = result[0]
    assert hasattr(content, "text")
    csv_text = content.text  # type: ignore[union-attr]
    # Skip leading `#`-prefixed comment lines (pandas-style convention); the
    # tool may prepend a `# Note: ...` line when description/metadata are
    # trimmed for broad listings.
    csv_body = "\n".join(
        line for line in csv_text.splitlines() if not line.startswith("#")
    )
    rows = list(csv.reader(io.StringIO(csv_body)))
    assert len(rows) > 1, "Expected header row plus at least one metric"
    assert "name" in rows[0]


async def test_remote_mcp_list_metrics_returns_metrics() -> None:
    async with session_context() as session:
        remote_metrics = await session.call_tool(
            name="list_metrics",
            arguments={},
        )
    assert not remote_metrics.isError
    assert len(remote_metrics.content) > 0
