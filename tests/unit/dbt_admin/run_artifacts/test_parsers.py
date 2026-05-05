"""Unit tests for ArtifactType enum and ARTIFACT_PARSERS dispatch table."""

import pytest

from dbt_mcp.dbt_admin.run_artifacts.artifacts.parsers import (
    ARTIFACT_PARSERS,
    ArtifactType,
)


class TestArtifactType:
    def test_all_four_artifact_types_present(self) -> None:
        assert {m.value for m in ArtifactType} == {
            "run_results.json",
            "sources.json",
            "manifest.json",
            "catalog.json",
        }

    def test_values_match_filenames(self) -> None:
        assert ArtifactType.RUN_RESULTS == "run_results.json"
        assert ArtifactType.SOURCES == "sources.json"
        assert ArtifactType.MANIFEST == "manifest.json"
        assert ArtifactType.CATALOG == "catalog.json"


class TestArtifactParsers:
    def test_covers_all_artifact_types(self) -> None:
        """Every ArtifactType member must have an entry in the dispatch table."""
        assert set(ARTIFACT_PARSERS.keys()) == set(ArtifactType)

    @pytest.mark.parametrize("artifact_type", list(ArtifactType))
    def test_parser_returns_dict_on_malformed_input(
        self, artifact_type: ArtifactType
    ) -> None:
        """Malformed artifact falls back to raw dict — no exception raised."""
        raw: dict = {"unexpected": "garbage"}
        result = ARTIFACT_PARSERS[artifact_type](raw)
        assert isinstance(result, dict)

    @pytest.mark.parametrize("artifact_type", list(ArtifactType))
    def test_parser_returns_raw_dict_on_empty_input(
        self, artifact_type: ArtifactType
    ) -> None:
        """Empty dict falls back to raw dict (strict parsing fails on missing fields)."""
        raw: dict = {}
        result = ARTIFACT_PARSERS[artifact_type](raw)
        assert result == {}
