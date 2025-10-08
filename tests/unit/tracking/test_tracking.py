import json
import uuid
from unittest.mock import patch

import pytest

from dbt_mcp.config.settings import AuthenticationMethod, DbtMcpSettings
from dbt_mcp.tools.tool_names import ToolName
from dbt_mcp.tools.toolsets import Toolset
from dbt_mcp.tracking.tracking import DefaultUsageTracker, ToolCalledEvent
from tests.mocks.config import MockCredentialsProvider


class TestUsageTracker:
    @pytest.mark.asyncio
    async def test_emit_tool_called_event_disabled(self):
        # Create settings with tracking explicitly disabled
        # usage_tracking_enabled is a property, so we need to set do_not_track
        mock_settings = DbtMcpSettings.model_construct(
            do_not_track="true",
        )

        tracker = DefaultUsageTracker(
            credentials_provider=MockCredentialsProvider(mock_settings),
            session_id=uuid.uuid4(),
        )

        with patch("dbt_mcp.tracking.tracking.log_proto") as mock_log_proto:
            await tracker.emit_tool_called_event(
                tool_called_event=ToolCalledEvent(
                    tool_name="list_metrics",
                    arguments={"foo": "bar"},
                    start_time_ms=0,
                    end_time_ms=1,
                    error_message=None,
                ),
            )

        mock_log_proto.assert_not_called()

    @pytest.mark.asyncio
    async def test_emit_tool_called_event_enabled(self):
        # Create settings with tracking enabled
        # usage_tracking_enabled is a property - tracking is enabled by default
        # when do_not_track and send_anonymous_usage_data are not set
        mock_settings = DbtMcpSettings.model_construct(
            do_not_track=None,  # Not disabled
            send_anonymous_usage_data=None,  # Not disabled
            dbt_prod_env_id=1,
            dbt_dev_env_id=2,
            dbt_user_id=3,
            actual_host="test.dbt.com",
            actual_host_prefix="prefix",
        )

        mock_credentials_provider = MockCredentialsProvider(mock_settings)

        tracker = DefaultUsageTracker(
            credentials_provider=mock_credentials_provider,
            session_id=uuid.uuid4(),
        )

        with (
            patch("uuid.uuid4", return_value="event-1"),
            patch("dbt_mcp.tracking.tracking.log_proto") as mock_log_proto,
            patch(
                "dbt_mcp.tracking.tracking.DefaultUsageTracker._get_local_user_id",
                return_value="local-user",
            ),
        ):
            await tracker.emit_tool_called_event(
                tool_called_event=ToolCalledEvent(
                    tool_name="list_metrics",
                    arguments={"foo": "bar"},
                    start_time_ms=0,
                    end_time_ms=1,
                    error_message=None,
                ),
            )

        mock_log_proto.assert_called_once()
        tool_called = mock_log_proto.call_args.args[0]
        assert tool_called.tool_name == "list_metrics"
        assert json.loads(tool_called.arguments["foo"]) == "bar"
        assert tool_called.dbt_cloud_environment_id_dev == "2"
        assert tool_called.dbt_cloud_environment_id_prod == "1"
        assert tool_called.dbt_cloud_user_id == "3"
        assert tool_called.local_user_id == "local-user"

    @pytest.mark.asyncio
    async def test_get_local_user_id_success(self):
        """Test loading local_user_id from .user.yml file"""
        mock_settings = DbtMcpSettings.model_construct(
            dbt_profiles_dir="/fake/profiles",
        )
        tracker = DefaultUsageTracker(
            credentials_provider=MockCredentialsProvider(mock_settings),
            session_id=uuid.uuid4(),
        )

        user_data = {"id": "user-123"}
        with patch("dbt_mcp.tracking.tracking.try_read_yaml", return_value=user_data):
            result = tracker._get_local_user_id(mock_settings)
            assert result == "user-123"

    @pytest.mark.asyncio
    async def test_get_local_user_id_caching(self):
        """Test that local_user_id is cached after first load"""
        mock_settings = DbtMcpSettings.model_construct(
            dbt_profiles_dir="/fake/profiles",
        )
        tracker = DefaultUsageTracker(
            credentials_provider=MockCredentialsProvider(mock_settings),
            session_id=uuid.uuid4(),
        )

        user_data = {"id": "user-123"}
        with patch(
            "dbt_mcp.tracking.tracking.try_read_yaml", return_value=user_data
        ) as mock_read:
            # First call should load from file
            result1 = tracker._get_local_user_id(mock_settings)
            assert result1 == "user-123"
            assert mock_read.call_count == 1

            # Second call should use cached value
            result2 = tracker._get_local_user_id(mock_settings)
            assert result2 == "user-123"
            assert mock_read.call_count == 1  # Not called again

    @pytest.mark.asyncio
    async def test_get_local_user_id_fusion_format(self):
        """Test handling of dbt Fusion format for .user.yml"""
        mock_settings = DbtMcpSettings.model_construct(
            dbt_profiles_dir="/fake/profiles",
        )
        tracker = DefaultUsageTracker(
            credentials_provider=MockCredentialsProvider(mock_settings),
            session_id=uuid.uuid4(),
        )

        # dbt Fusion may return a string directly instead of a dict
        user_data = "user-fusion-456"
        with patch("dbt_mcp.tracking.tracking.try_read_yaml", return_value=user_data):
            result = tracker._get_local_user_id(mock_settings)
            assert result == "user-fusion-456"

    @pytest.mark.asyncio
    async def test_get_local_user_id_no_file(self):
        """Test behavior when .user.yml doesn't exist - should generate new UUID"""
        mock_settings = DbtMcpSettings.model_construct(
            dbt_profiles_dir="/fake/profiles",
        )
        tracker = DefaultUsageTracker(
            credentials_provider=MockCredentialsProvider(mock_settings),
            session_id=uuid.uuid4(),
        )

        with patch("dbt_mcp.tracking.tracking.try_read_yaml", return_value=None):
            result = tracker._get_local_user_id(mock_settings)
            # When file doesn't exist, a new UUID should be generated
            assert result is not None
            # Verify it's a valid UUID string
            uuid.UUID(result)  # This will raise ValueError if invalid

    @pytest.mark.asyncio
    async def test_get_settings_caching(self):
        """Test that settings are cached after first retrieval"""
        mock_settings = DbtMcpSettings.model_construct(
            dbt_prod_env_id=123,
        )
        mock_credentials_provider = MockCredentialsProvider(mock_settings)

        tracker = DefaultUsageTracker(
            credentials_provider=mock_credentials_provider,
            session_id=uuid.uuid4(),
        )

        # Mock the credentials provider to track call count
        original_get_credentials = mock_credentials_provider.get_credentials
        call_count = 0

        async def tracked_get_credentials():
            nonlocal call_count
            call_count += 1
            return await original_get_credentials()

        mock_credentials_provider.get_credentials = tracked_get_credentials

        # First call should fetch settings
        settings1 = await tracker._get_settings()
        assert settings1.dbt_prod_env_id == 123
        assert call_count == 1

        # Second call should use cached settings
        settings2 = await tracker._get_settings()
        assert settings2.dbt_prod_env_id == 123
        assert call_count == 1  # Not called again

    @pytest.mark.asyncio
    async def test_get_disabled_toolsets_none_disabled(self):
        """Test when all toolsets are enabled"""
        mock_settings = DbtMcpSettings.model_construct(
            disable_sql=False,
            disable_semantic_layer=False,
            disable_discovery=False,
            disable_dbt_cli=False,
            disable_admin_api=False,
            disable_dbt_codegen=False,
        )
        tracker = DefaultUsageTracker(
            credentials_provider=MockCredentialsProvider(mock_settings),
            session_id=uuid.uuid4(),
        )

        disabled = tracker._get_disabled_toolsets(mock_settings)
        assert disabled == []

    @pytest.mark.asyncio
    async def test_get_disabled_toolsets_some_disabled(self):
        """Test when some toolsets are disabled"""
        mock_settings = DbtMcpSettings.model_construct(
            disable_sql=True,
            disable_semantic_layer=True,
            disable_discovery=False,
            disable_dbt_cli=False,
            disable_admin_api=False,
            disable_dbt_codegen=False,
        )
        tracker = DefaultUsageTracker(
            credentials_provider=MockCredentialsProvider(mock_settings),
            session_id=uuid.uuid4(),
        )

        disabled = tracker._get_disabled_toolsets(mock_settings)
        assert set(disabled) == {Toolset.SQL, Toolset.SEMANTIC_LAYER}

    @pytest.mark.asyncio
    async def test_get_disabled_toolsets_all_disabled(self):
        """Test when all toolsets are disabled"""
        mock_settings = DbtMcpSettings.model_construct(
            disable_sql=True,
            disable_semantic_layer=True,
            disable_discovery=True,
            disable_dbt_cli=True,
            disable_admin_api=True,
            disable_dbt_codegen=True,
        )
        tracker = DefaultUsageTracker(
            credentials_provider=MockCredentialsProvider(mock_settings),
            session_id=uuid.uuid4(),
        )

        disabled = tracker._get_disabled_toolsets(mock_settings)
        assert set(disabled) == {
            Toolset.SQL,
            Toolset.SEMANTIC_LAYER,
            Toolset.DISCOVERY,
            Toolset.DBT_CLI,
            Toolset.ADMIN_API,
            Toolset.DBT_CODEGEN,
        }

    @pytest.mark.asyncio
    async def test_emit_tool_called_event_includes_authentication_method(self):
        """Test that authentication_method is included in the event"""
        mock_settings = DbtMcpSettings.model_construct(
            do_not_track=None,
            send_anonymous_usage_data=None,
            dbt_prod_env_id=1,
        )
        mock_credentials_provider = MockCredentialsProvider(mock_settings)
        mock_credentials_provider.authentication_method = AuthenticationMethod.ENV_VAR

        tracker = DefaultUsageTracker(
            credentials_provider=mock_credentials_provider,
            session_id=uuid.uuid4(),
        )

        with (
            patch("dbt_mcp.tracking.tracking.log_proto") as mock_log_proto,
            patch(
                "dbt_mcp.tracking.tracking.DefaultUsageTracker._get_local_user_id",
                return_value=None,
            ),
        ):
            await tracker.emit_tool_called_event(
                tool_called_event=ToolCalledEvent(
                    tool_name="test_tool",
                    arguments={},
                    start_time_ms=0,
                    end_time_ms=1,
                    error_message=None,
                ),
            )

        mock_log_proto.assert_called_once()
        tool_called = mock_log_proto.call_args.args[0]
        assert tool_called.authentication_method == "env_var"

    @pytest.mark.asyncio
    async def test_emit_tool_called_event_includes_disabled_toolsets(self):
        """Test that disabled_toolsets are included in the event"""
        mock_settings = DbtMcpSettings.model_construct(
            do_not_track=None,
            send_anonymous_usage_data=None,
            disable_sql=True,
            disable_semantic_layer=True,
            disable_discovery=False,
            disable_dbt_cli=False,
            disable_admin_api=False,
            disable_dbt_codegen=False,
        )
        mock_credentials_provider = MockCredentialsProvider(mock_settings)

        tracker = DefaultUsageTracker(
            credentials_provider=mock_credentials_provider,
            session_id=uuid.uuid4(),
        )

        with (
            patch("dbt_mcp.tracking.tracking.log_proto") as mock_log_proto,
            patch(
                "dbt_mcp.tracking.tracking.DefaultUsageTracker._get_local_user_id",
                return_value=None,
            ),
        ):
            await tracker.emit_tool_called_event(
                tool_called_event=ToolCalledEvent(
                    tool_name="test_tool",
                    arguments={},
                    start_time_ms=0,
                    end_time_ms=1,
                    error_message=None,
                ),
            )

        mock_log_proto.assert_called_once()
        tool_called = mock_log_proto.call_args.args[0]
        assert set(tool_called.disabled_toolsets) == {"sql", "semantic_layer"}

    @pytest.mark.asyncio
    async def test_emit_tool_called_event_includes_disabled_tools(self):
        """Test that disabled_tools are included in the event"""
        mock_settings = DbtMcpSettings.model_construct(
            do_not_track=None,
            send_anonymous_usage_data=None,
            disable_tools=[ToolName.BUILD, ToolName.RUN],
        )
        mock_credentials_provider = MockCredentialsProvider(mock_settings)

        tracker = DefaultUsageTracker(
            credentials_provider=mock_credentials_provider,
            session_id=uuid.uuid4(),
        )

        with (
            patch("dbt_mcp.tracking.tracking.log_proto") as mock_log_proto,
            patch(
                "dbt_mcp.tracking.tracking.DefaultUsageTracker._get_local_user_id",
                return_value=None,
            ),
        ):
            await tracker.emit_tool_called_event(
                tool_called_event=ToolCalledEvent(
                    tool_name="test_tool",
                    arguments={},
                    start_time_ms=0,
                    end_time_ms=1,
                    error_message=None,
                ),
            )

        mock_log_proto.assert_called_once()
        tool_called = mock_log_proto.call_args.args[0]
        assert set(tool_called.disabled_tools) == {"build", "run"}

    @pytest.mark.asyncio
    async def test_emit_tool_called_event_includes_session_id(self):
        """Test that session_id is included in the event context"""
        mock_settings = DbtMcpSettings.model_construct(
            do_not_track=None,
            send_anonymous_usage_data=None,
        )
        mock_credentials_provider = MockCredentialsProvider(mock_settings)

        session_id = uuid.uuid4()
        tracker = DefaultUsageTracker(
            credentials_provider=mock_credentials_provider,
            session_id=session_id,
        )

        with (
            patch("dbt_mcp.tracking.tracking.log_proto") as mock_log_proto,
            patch(
                "dbt_mcp.tracking.tracking.DefaultUsageTracker._get_local_user_id",
                return_value=None,
            ),
        ):
            await tracker.emit_tool_called_event(
                tool_called_event=ToolCalledEvent(
                    tool_name="test_tool",
                    arguments={},
                    start_time_ms=0,
                    end_time_ms=1,
                    error_message=None,
                ),
            )

        mock_log_proto.assert_called_once()
        tool_called = mock_log_proto.call_args.args[0]
        assert tool_called.ctx.session_id == str(session_id)

    @pytest.mark.asyncio
    async def test_emit_tool_called_event_includes_dbt_mcp_version(self):
        """Test that dbt_mcp_version is included in the event"""
        mock_settings = DbtMcpSettings.model_construct(
            do_not_track=None,
            send_anonymous_usage_data=None,
        )
        mock_credentials_provider = MockCredentialsProvider(mock_settings)

        tracker = DefaultUsageTracker(
            credentials_provider=mock_credentials_provider,
            session_id=uuid.uuid4(),
        )

        with (
            patch("dbt_mcp.tracking.tracking.log_proto") as mock_log_proto,
            patch(
                "dbt_mcp.tracking.tracking.DefaultUsageTracker._get_local_user_id",
                return_value=None,
            ),
        ):
            await tracker.emit_tool_called_event(
                tool_called_event=ToolCalledEvent(
                    tool_name="test_tool",
                    arguments={},
                    start_time_ms=0,
                    end_time_ms=1,
                    error_message=None,
                ),
            )

        mock_log_proto.assert_called_once()
        tool_called = mock_log_proto.call_args.args[0]
        # Just verify the field exists, don't assert specific version
        assert hasattr(tool_called, "dbt_mcp_version")
        assert isinstance(tool_called.dbt_mcp_version, str)
