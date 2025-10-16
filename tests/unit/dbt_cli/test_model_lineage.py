import pytest

from dbt_mcp.discovery.models.lineage_types import ModelLineage


@pytest.fixture
def sample_manifest():
    yield {
        "child_map": {
            "model.a": ["model.b", "model.c"],
            "model.b": ["model.d", "test.not_included"],
            "model.c": [],
            "model.d": [],
            "source.1": ["model.a"],
        },
        "parent_map": {
            "model.b": ["model.a"],
            "model.c": ["model.a"],
            "model.d": ["model.b"],
            "model.a": ["source.1"],
            "source.1": [],
        },
    }


def test_model_lineage_a__from_manifest(sample_manifest):
    manifest = sample_manifest
    lineage = ModelLineage.from_manifest(
        manifest, "model.a", direction="both", recursive=True
    )
    assert lineage.model_id == "model.a"
    assert lineage.parents[0].model_id == "source.1", (
        "Expected source.1 as parent to model.a"
    )
    assert len(lineage.children) == 2, "Expected 2 children for model.a"
    model_b = lineage.children[0]
    assert model_b.model_id == "model.b", "Expected model.b as first child of model.a"
    assert len(model_b.children) == 1, (
        "Expect test.not_included to be excluded from children of model.b"
    )
    assert model_b.children[0].model_id == "model.d", (
        "Expected model.d as child of model.b"
    )


def test_model_lineage_b__from_manifest(sample_manifest):
    manifest = sample_manifest
    lineage_b = ModelLineage.from_manifest(
        manifest, "model.b", direction="parents", recursive=True
    )
    assert lineage_b.model_id == "model.b"
    assert len(lineage_b.parents) == 1, "Expected 1 parent for model.b"

    assert len(lineage_b.children) == 0, (
        "Expected no children when only fetching parents"
    )


def test_model_lineage__from_manifest_with_tests(sample_manifest):
    manifest = sample_manifest

    lineage = ModelLineage.from_manifest(
        manifest, "model.a", direction="children", recursive=True, exclude_prefixes=()
    )
    assert len(lineage.children) == 2, "Expected 2 children for model.a"
    model_b = lineage.children[0]
    assert model_b.model_id == "model.b", "Expected model.b as first child of model.a"
    assert len(model_b.children) == 2, "Expected 2 children for model.b including tests"
    assert lineage.children[0].children[1].model_id == "test.not_included"
    assert len(lineage.parents) == 0, "Expected no parents when only fetching children"
