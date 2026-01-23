from __future__ import annotations
from typing import cast

from pydantic import BaseModel, Field

from dbt_mcp.dbt_cli.models.manifest import Manifest
from dbt_mcp.tools.fields import LineageResourceType

# These resource types are not supported in the CLI
UNALLOWED_RESOURCE_TYPES = {
    LineageResourceType.SEMANTIC_MODEL,
    LineageResourceType.SAVED_QUERY,
}


def _resource_types_to_prefix(resource: LineageResourceType) -> tuple[str, ...]:
    """Convert LineageResourceType to tuple of unique_id prefixes."""
    if resource in UNALLOWED_RESOURCE_TYPES:
        raise ValueError(
            f"Resource types '{', '.join(UNALLOWED_RESOURCE_TYPES)}' are not supported for lineage in dbt CLI."
        )
    if resource == LineageResourceType.TEST:
        return ("test.", "unit_test.")

    return (f"{resource.lower()}.",)


def _get_include_prefixes(
    types: list[LineageResourceType] | None,
) -> tuple[str, ...] | None:
    """Convert types list to tuple of prefixes to include. None means include all."""
    if types is None:
        return None
    prefixes: list[str] = []
    for t in types:
        prefixes.extend(_resource_types_to_prefix(t))
    return tuple(prefixes)


def _should_include(node_id: str, include_prefixes: tuple[str, ...] | None) -> bool:
    """Check if a node should be included based on its prefix."""
    if include_prefixes is None:
        return True
    return node_id.startswith(include_prefixes)


class Descendant(BaseModel):
    model_id: str
    children: list[Descendant] = Field(default_factory=list)


class Ancestor(BaseModel):
    model_id: str
    parents: list[Ancestor] = Field(default_factory=list)


class ModelLineage(BaseModel):
    model_id: str
    parents: list[Ancestor] = Field(default_factory=list)
    children: list[Descendant] = Field(default_factory=list)

    @classmethod
    def from_manifest(
        cls,
        manifest: Manifest,
        unique_id: str,
        types: list[LineageResourceType] | None = None,
        depth: int = 5,
    ) -> ModelLineage:
        """
        Build a ModelLineage instance from a dbt manifest mapping.

        - manifest: Manifest object containing at least 'parent_map' and/or 'child_map'
        - unique_id: the fully-qualified unique ID to start from
        - types: list of resource types to include. If None, includes all types.
        - depth: how many levels to traverse (0 = infinite, 1 = immediate neighbors only, higher = deeper)

        The returned ModelLineage contains lists of Ancestor and/or Descendant
        objects.
        """
        parent_map = manifest.parent_map
        child_map = manifest.child_map
        include_prefixes = _get_include_prefixes(types)

        parents: list[Ancestor] = []
        children: list[Descendant] = []

        def _build_node(
            node_id: str,
            map_data: dict[str, list[str]],
            key: str,
            path: set[str],
            current_depth: int,
        ) -> Ancestor | Descendant | None:
            if node_id in path:
                return None

            next_nodes: list[Ancestor | Descendant] = []
            if depth == 0 or current_depth < depth:
                for next_id in map_data.get(node_id, []):
                    if not _should_include(next_id, include_prefixes):
                        continue
                    child_node = _build_node(
                        next_id, map_data, key, path | {node_id}, current_depth + 1
                    )
                    if child_node:
                        next_nodes.append(child_node)
            if key == "parents":
                return Ancestor(
                    model_id=node_id, parents=cast(list[Ancestor], next_nodes)
                )
            return Descendant(
                model_id=node_id, children=cast(list[Descendant], next_nodes)
            )

        for item_id in parent_map.get(unique_id, []):
            if not _should_include(item_id, include_prefixes):
                continue

            if depth == 0 or depth > 1:
                p_node = _build_node(item_id, parent_map, "parents", {unique_id}, 1)
                if p_node:
                    parents.append(cast(Ancestor, p_node))
            else:
                parents.append(Ancestor(model_id=item_id))

        for item_id in child_map.get(unique_id, []):
            if not _should_include(item_id, include_prefixes):
                continue

            if depth == 0 or depth > 1:
                c_node = _build_node(item_id, child_map, "children", {unique_id}, 1)
                if c_node:
                    children.append(cast(Descendant, c_node))
            else:
                children.append(Descendant(model_id=item_id))

        return cls(
            model_id=unique_id,
            parents=parents,
            children=children,
        )
