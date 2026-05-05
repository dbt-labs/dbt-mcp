"""Unit tests for lenient Pydantic fallback schemas."""

from pydantic import BaseModel

from dbt_mcp.dbt_admin.run_artifacts.artifacts.lenient import (
    LenientCatalog,
    LenientManifest,
    LenientRunResults,
    LenientRunResultsArgs,
    LenientRunResultsResult,
    LenientSourceResult,
    LenientSources,
)


class TestLenientRunResultsResult:
    def test_valid_result(self) -> None:
        r = LenientRunResultsResult.model_validate(
            {"status": "error", "unique_id": "model.proj.foo", "message": "boom"}
        )
        assert r.status == "error"
        assert r.unique_id == "model.proj.foo"

    def test_unknown_status_passes(self) -> None:
        """'reused' and other undocumented statuses must not raise."""
        r = LenientRunResultsResult.model_validate({"status": "reused"})
        assert r.status == "reused"

    def test_null_status_passes(self) -> None:
        r = LenientRunResultsResult.model_validate({"status": None})
        assert r.status is None

    def test_all_fields_optional(self) -> None:
        r = LenientRunResultsResult.model_validate({})
        assert r.status is None
        assert r.unique_id is None
        assert r.compiled_code is None
        assert r.compiled_sql is None

    def test_extra_fields_allowed(self) -> None:
        r = LenientRunResultsResult.model_validate({"status": "warn", "timing": []})
        assert r.status == "warn"

    def test_compiled_sql_field(self) -> None:
        """Older dbt versions emit compiled_sql; it must be directly accessible."""
        r = LenientRunResultsResult.model_validate({"compiled_sql": "SELECT 1"})
        assert r.compiled_sql == "SELECT 1"

    def test_is_pydantic_model(self) -> None:
        assert isinstance(LenientRunResultsResult.model_validate({}), BaseModel)


class TestLenientRunResults:
    def test_valid_run_results(self) -> None:
        parsed = LenientRunResults.model_validate(
            {
                "results": [{"status": "error", "unique_id": "model.proj.foo"}],
                "args": {"target": "prod"},
            }
        )
        assert len(parsed.results) == 1
        assert isinstance(parsed.results[0], LenientRunResultsResult)
        assert parsed.results[0].status == "error"
        assert isinstance(parsed.args, LenientRunResultsArgs)
        assert parsed.args.target == "prod"

    def test_null_results_coerced_to_empty_list(self) -> None:
        parsed = LenientRunResults.model_validate({"results": None})
        assert parsed.results == []

    def test_missing_results_defaults_to_empty(self) -> None:
        parsed = LenientRunResults.model_validate({})
        assert parsed.results == []

    def test_missing_args_is_none(self) -> None:
        parsed = LenientRunResults.model_validate({})
        assert parsed.args is None

    def test_is_pydantic_model(self) -> None:
        assert isinstance(LenientRunResults.model_validate({}), BaseModel)

    def test_extra_fields_allowed(self) -> None:
        parsed = LenientRunResults.model_validate({"elapsed_time": 1.5})
        assert parsed.model_extra is not None
        assert parsed.model_extra.get("elapsed_time") == 1.5


class TestLenientSources:
    def test_valid_sources(self) -> None:
        parsed = LenientSources.model_validate(
            {
                "results": [
                    {
                        "status": "error",
                        "unique_id": "source.proj.raw.users",
                        "max_loaded_at_time_ago_in_s": 172800.0,
                    }
                ]
            }
        )
        assert len(parsed.results) == 1
        assert isinstance(parsed.results[0], LenientSourceResult)
        assert parsed.results[0].status == "error"
        assert parsed.results[0].max_loaded_at_time_ago_in_s == 172800.0

    def test_null_results_coerced(self) -> None:
        assert LenientSources.model_validate({"results": None}).results == []

    def test_missing_results_defaults_to_empty(self) -> None:
        assert LenientSources.model_validate({}).results == []

    def test_is_pydantic_model(self) -> None:
        assert isinstance(LenientSources.model_validate({}), BaseModel)


class TestLenientCatalog:
    def test_nodes_and_sources_populated(self) -> None:
        parsed = LenientCatalog.model_validate(
            {"nodes": {"node.a": {}}, "sources": {"source.b": {}}}
        )
        assert "node.a" in parsed.nodes
        assert "source.b" in parsed.sources

    def test_null_nodes_coerced_to_empty(self) -> None:
        parsed = LenientCatalog.model_validate({"nodes": None, "sources": None})
        assert parsed.nodes == {}
        assert parsed.sources == {}

    def test_missing_fields_default_to_empty(self) -> None:
        parsed = LenientCatalog.model_validate({})
        assert parsed.nodes == {}
        assert parsed.sources == {}

    def test_is_pydantic_model(self) -> None:
        assert isinstance(LenientCatalog.model_validate({}), BaseModel)


class TestLenientManifest:
    def test_nodes_and_sources_populated(self) -> None:
        parsed = LenientManifest.model_validate(
            {"nodes": {"model.proj.foo": {}}, "sources": {}}
        )
        assert "model.proj.foo" in parsed.nodes

    def test_null_fields_coerced(self) -> None:
        parsed = LenientManifest.model_validate({"nodes": None, "sources": None})
        assert parsed.nodes == {}
        assert parsed.sources == {}

    def test_is_pydantic_model(self) -> None:
        assert isinstance(LenientManifest.model_validate({}), BaseModel)
