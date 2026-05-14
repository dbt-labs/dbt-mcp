import csv
import io
from itertools import dropwhile

from mcp.types import ContentBlock, TextContent

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
    # call_tool returns (content_list, structured_dict) when structured_output=True
    content_list: list[ContentBlock]
    content_list, _ = result  # type: ignore[assignment]
    assert len(content_list) == 1
    content = content_list[0]
    assert isinstance(content, TextContent)
    csv_text = content.text
    # Drop only the leading block of `#`-prefixed comment lines (pandas-style
    # convention); the tool may prepend a `# Note: ...` line when
    # description/metadata are trimmed for broad listings. `dropwhile` stops at
    # the first non-`#` line so a value that happens to contain `#` later in
    # the CSV isn't silently discarded.
    body_lines = dropwhile(lambda line: line.startswith("#"), csv_text.splitlines())
    csv_body = "\n".join(body_lines)
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
