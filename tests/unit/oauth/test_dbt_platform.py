from dbt_mcp.oauth.dbt_platform import DbtPlatformContext, DbtPlatformEnvironment


class TestDbtPlatformContextOverride:
    def _env(self, id: int) -> DbtPlatformEnvironment:
        return DbtPlatformEnvironment(
            id=id, name=f"env-{id}", deployment_type="production"
        )

    def test_multi_project_clears_inherited_environments(self) -> None:
        """Transitioning from single-project to multi-project must not carry
        forward prod/dev environments — they would create an invalid mixed state
        where both dbt_prod_env_id and dbt_project_ids are set."""
        base = DbtPlatformContext(
            prod_environment=self._env(100),
            dev_environment=self._env(200),
            account_id=1,
        )
        new = DbtPlatformContext(
            selected_project_ids=[10, 20],
            account_id=1,
        )
        result = base.override(new)
        assert result.selected_project_ids == [10, 20]
        assert result.prod_environment is None
        assert result.dev_environment is None

    def test_single_project_inherits_environments(self) -> None:
        """When the new context has no selected_project_ids, existing
        environments should still be inherited."""
        base = DbtPlatformContext(
            prod_environment=self._env(100),
            dev_environment=self._env(200),
            account_id=1,
        )
        new = DbtPlatformContext(account_id=1)
        result = base.override(new)
        assert result.prod_environment == self._env(100)
        assert result.dev_environment == self._env(200)
        assert result.selected_project_ids is None

    def test_new_environments_take_precedence(self) -> None:
        """Explicitly set environments in the new context always win."""
        base = DbtPlatformContext(
            prod_environment=self._env(100),
            dev_environment=self._env(200),
        )
        new = DbtPlatformContext(
            prod_environment=self._env(300),
            dev_environment=self._env(400),
        )
        result = base.override(new)
        assert result.prod_environment == self._env(300)
        assert result.dev_environment == self._env(400)
