import json
from unittest.mock import patch

import pytest

from dbt_mcp.config.config import TrackingConfig
from dbt_mcp.tracking.tracking import UsageTracker


@pytest.fixture
def tracking_config() -> TrackingConfig:
    return TrackingConfig(
        host="test.dbt.com",
        host_prefix="prefix",
        prod_environment_id=1,
        dev_environment_id=2,
        dbt_cloud_user_id=3,
        local_user_id="local-user",
    )


class TestUsageTracker:
    def test_emit_tool_called_event_disabled(self, tracking_config):
        tracking_config.usage_tracking_enabled = False

        tracker = UsageTracker()

        with patch("dbt_mcp.tracking.tracking.log_proto") as mock_log_proto:
            tracker.emit_tool_called_event(
                config=tracking_config,
                tool_name="list_metrics",
                arguments={"foo": "bar"},
                start_time_ms=0,
                end_time_ms=1,
            )

        mock_log_proto.assert_not_called()

    def test_emit_tool_called_event_enabled(self, tracking_config):
        tracking_config.usage_tracking_enabled = True

        tracker = UsageTracker()

        with patch("uuid.uuid4", return_value="event-1"):
            with patch("dbt_mcp.tracking.tracking.log_proto") as mock_log_proto:
                tracker.emit_tool_called_event(
                    config=tracking_config,
                    tool_name="list_metrics",
                    arguments={"foo": "bar"},
                    start_time_ms=0,
                    end_time_ms=1,
                    error_message=None,
                )

        mock_log_proto.assert_called_once()
        tool_called = mock_log_proto.call_args.args[0]
        assert tool_called.tool_name == "list_metrics"
        assert json.loads(tool_called.arguments["foo"]) == "bar"
        assert tool_called.dbt_cloud_environment_id_dev == "2"
        assert tool_called.dbt_cloud_environment_id_prod == "1"
        assert tool_called.dbt_cloud_user_id == "3"
        assert tool_called.local_user_id == "local-user"
