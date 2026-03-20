from unittest.mock import AsyncMock, patch

import pytest

from dbt_mcp.config.config import DbtCliConfig
from dbt_mcp.dbt_cli.auth_check import AuthStatus, WarehouseAuthChecker
from dbt_mcp.dbt_cli.binary_type import BinaryType


@pytest.fixture
def cli_config() -> DbtCliConfig:
    return DbtCliConfig(
        project_dir="/tmp/test_project",
        dbt_path="/usr/local/bin/dbt",
        dbt_cli_timeout=60,
        binary_type=BinaryType.DBT_CORE,
    )


@pytest.fixture
def auth_checker(cli_config: DbtCliConfig) -> WarehouseAuthChecker:
    return WarehouseAuthChecker(cli_config)


class TestWarehouseAuthChecker:
    def test_initial_status_is_not_started(
        self, auth_checker: WarehouseAuthChecker
    ) -> None:
        assert auth_checker.status == AuthStatus.NOT_STARTED
        assert auth_checker.error_message is None

    def test_get_status_message_when_not_started(
        self, auth_checker: WarehouseAuthChecker
    ) -> None:
        assert auth_checker.get_status_message() is None

    def test_get_status_message_when_authenticated(
        self, auth_checker: WarehouseAuthChecker
    ) -> None:
        auth_checker._status = AuthStatus.AUTHENTICATED
        assert auth_checker.get_status_message() is None

    def test_get_status_message_when_in_progress(
        self, auth_checker: WarehouseAuthChecker
    ) -> None:
        auth_checker._status = AuthStatus.IN_PROGRESS
        assert auth_checker.get_status_message() is None

    def test_get_status_message_when_timeout(
        self, auth_checker: WarehouseAuthChecker
    ) -> None:
        auth_checker._status = AuthStatus.TIMEOUT
        auth_checker._error_message = "timed out"
        message = auth_checker.get_status_message()
        assert message == "timed out"

    def test_get_status_message_when_failed(
        self, auth_checker: WarehouseAuthChecker
    ) -> None:
        auth_checker._status = AuthStatus.FAILED
        auth_checker._error_message = "connection refused"
        message = auth_checker.get_status_message()
        assert message is not None
        assert "connection refused" in message

    @pytest.mark.asyncio
    async def test_successful_auth_check(
        self, auth_checker: WarehouseAuthChecker
    ) -> None:
        mock_process = AsyncMock()
        mock_process.communicate.return_value = (b"All checks passed!", None)
        mock_process.returncode = 0

        with patch(
            "dbt_mcp.dbt_cli.auth_check.asyncio.create_subprocess_exec",
            return_value=mock_process,
        ):
            await auth_checker.run_auth_check()

        assert auth_checker.status == AuthStatus.AUTHENTICATED
        assert auth_checker.error_message is None

    @pytest.mark.asyncio
    async def test_failed_auth_check(self, auth_checker: WarehouseAuthChecker) -> None:
        mock_process = AsyncMock()
        mock_process.communicate.return_value = (b"Connection failed", None)
        mock_process.returncode = 1

        with patch(
            "dbt_mcp.dbt_cli.auth_check.asyncio.create_subprocess_exec",
            return_value=mock_process,
        ):
            await auth_checker.run_auth_check()

        assert auth_checker.status == AuthStatus.FAILED
        assert "Connection failed" in (auth_checker.error_message or "")

    @pytest.mark.asyncio
    async def test_timeout_auth_check(self, auth_checker: WarehouseAuthChecker) -> None:
        mock_process = AsyncMock()
        mock_process.communicate.side_effect = TimeoutError()

        with patch(
            "dbt_mcp.dbt_cli.auth_check.asyncio.create_subprocess_exec",
            return_value=mock_process,
        ):
            await auth_checker.run_auth_check()

        assert auth_checker.status == AuthStatus.TIMEOUT
        assert "browser-based authentication" in (auth_checker.error_message or "")

    @pytest.mark.asyncio
    async def test_exception_auth_check(
        self, auth_checker: WarehouseAuthChecker
    ) -> None:
        with patch(
            "dbt_mcp.dbt_cli.auth_check.asyncio.create_subprocess_exec",
            side_effect=FileNotFoundError("dbt not found"),
        ):
            await auth_checker.run_auth_check()

        assert auth_checker.status == AuthStatus.FAILED
        assert "dbt not found" in (auth_checker.error_message or "")
