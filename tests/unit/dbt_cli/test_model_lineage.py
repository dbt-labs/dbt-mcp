import pytest

from dbt_mcp.dbt_cli.models.lineage_types import ModelLineage, Descendant
from dbt_mcp.dbt_cli.models.manifest import Manifest
from dbt_mcp.tools.parameters import LineageResourceType


@pytest.fixture
def sample_manifest():
    data = {
        "child_map": {
            "model.a": ["model.b", "model.c"],
            "model.b": ["model.d", "test.some_test_14"],
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
    yield Manifest(**data)


@pytest.mark.parametrize(
    "types",
    [
        pytest.param(
            [LineageResourceType.MODEL, LineageResourceType.SOURCE],
            id="using_enum_types",
        ),
        pytest.param(["Model", "Source"], id="using_string_types"),
    ],
)
def test_model_lineage_a__from_manifest(sample_manifest, types):
    """Test basic lineage fetching with source and models"""
    manifest = sample_manifest
    lineage = ModelLineage.from_manifest(manifest, "model.a", types=types, depth=5)
    assert lineage.model_id == "model.a"
    assert lineage.parents[0].model_id == "source.1", (
        "Expected source.1 as parent to model.a"
    )
    assert len(lineage.children) == 2, "Expected 2 children for model.a"
    model_b = lineage.children[0]
    assert model_b.model_id == "model.b", "Expected model.b as first child of model.a"
    assert len(model_b.children) == 1, (
        "Expect test.some_test_14 to be excluded from children of model.b"
    )
    assert model_b.children[0].model_id == "model.d", (
        "Expected model.d as child of model.b"
    )


def test_model_lineage_b__from_manifest(sample_manifest):
    """Test lineage from a mid-graph node."""
    manifest = sample_manifest
    lineage_b = ModelLineage.from_manifest(manifest, "model.b", depth=5)
    assert lineage_b.model_id == "model.b"
    assert len(lineage_b.parents) == 1, "Expected 1 parent for model.b"
    assert lineage_b.parents[0].model_id == "model.a"
    assert len(lineage_b.children) == 2, "Expected 2 children for model.b"
    child_ids = {c.model_id for c in lineage_b.children}
    assert child_ids == {"model.d", "test.some_test_14"}


def test_model_lineage__from_manifest_with_tests(sample_manifest):
    """Test lineage including test resources."""
    manifest = sample_manifest

    # Include MODEL, SOURCE, and TEST types
    types = [
        LineageResourceType.MODEL,
        LineageResourceType.SOURCE,
        LineageResourceType.TEST,
    ]
    lineage = ModelLineage.from_manifest(manifest, "model.a", types=types, depth=5)
    assert len(lineage.children) == 2, "Expected 2 children for model.a"
    model_b = lineage.children[0]
    assert model_b.model_id == "model.b", "Expected model.b as first child of model.a"
    assert len(model_b.children) == 2, "Expected 2 children for model.b including tests"
    assert lineage.children[0].children[1].model_id == "test.some_test_14"
    # Parents should also be included
    assert len(lineage.parents) == 1, "Expected 1 parent for model.a"
    assert lineage.parents[0].model_id == "source.1"


def test_model_lineage__depth_one_returns_immediate_only(sample_manifest):
    """Test that depth=1 only returns immediate neighbors without nested children/parents."""
    manifest = sample_manifest

    lineage = ModelLineage.from_manifest(manifest, "model.a", depth=1)
    assert lineage.model_id == "model.a"
    # Should have immediate children but no nested children
    assert len(lineage.children) == 2, "Expected 2 immediate children for model.a"
    for child in lineage.children:
        assert len(child.children) == 0, (
            f"Expected no nested children for {child.model_id} at depth=1"
        )
    # Should have immediate parent but no nested parents
    assert len(lineage.parents) == 1, "Expected 1 immediate parent for model.a"
    assert lineage.parents[0].model_id == "source.1"
    assert len(lineage.parents[0].parents) == 0, "Expected no nested parents at depth=1"


def test_model_lineage__types_filter_models_only(sample_manifest):
    """Test that types filter works to include only specific resource types."""
    manifest = sample_manifest

    # Only include models - should exclude sources
    lineage = ModelLineage.from_manifest(
        manifest, "model.a", types=[LineageResourceType.MODEL], depth=5
    )
    assert lineage.model_id == "model.a"
    # Parents should be empty since source.1 is not a model
    assert len(lineage.parents) == 0, (
        "Expected no parents when only MODEL type included"
    )
    # Children should still have models
    assert len(lineage.children) == 2, "Expected 2 model children for model.a"


def test_model_lineage__depth_zero_is_infinite(sample_manifest):
    """Test that depth=0 returns all nodes in the graph."""
    manifest = sample_manifest
    # source.1 -> model.a -> (model.b -> model.d, model.c)
    lineage = ModelLineage.from_manifest(manifest, "source.1", depth=0)

    # flatten the children to get all unique IDs
    def collect_children(
        node: ModelLineage | Descendant, collected: set[str] | None = None
    ) -> set[str]:
        if collected is None:
            collected = set()
        else:
            collected.add(node.model_id)
        for child in getattr(node, "children", []):
            collect_children(child, collected)
        return collected

    all_ids = collect_children(lineage)

    expected_ids = {"model.a", "model.b", "model.c", "model.d", "test.some_test_14"}

    assert all_ids == expected_ids, "Expected all nodes in the graph with depth=0"
