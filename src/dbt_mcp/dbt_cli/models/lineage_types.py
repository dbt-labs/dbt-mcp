from __future__ import annotations
from typing import Literal, cast

from pydantic import BaseModel, Field

from dbt_mcp.dbt_cli.models.manifest import Manifest


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
        model_id: str,
        direction: Literal["parents", "children", "both"] = "both",
        exclude_prefixes: tuple[str, ...] = ("test.", "unit_test."),
        *,
        recursive: bool = False,
    ) -> ModelLineage:
        """
        Build a ModelLineage instance from a dbt manifest mapping.

        - manifest: Manifest object containing at least 'parent_map' and/or 'child_map'
        - model_id: the model id to start from
        - recursive: whether to traverse recursively
        - direction: one of 'parents', 'children', or 'both'
        - exclude_prefixes: tuple of prefixes to exclude from descendants, defaults to ("test.", "unit_test.")
            Descendants only. Give () to include all.

        The returned ModelLineage contains lists of Ancestor and/or Descendant
        objects.
        """
        parent_map = manifest.parent_map
        child_map = manifest.child_map

        parents: list[Ancestor] = []
        children: list[Descendant] = []
        model_id = get_uid_from_name(manifest, model_id)

        def _build_node(
            node_id: str,
            map_data: dict[str, list[str]],
            key: str,
            path: set[str],
        ) -> Ancestor | Descendant | None:
            if node_id in path:
                return None

            next_nodes: list[Ancestor | Descendant] = []
            for next_id in map_data.get(node_id, []):
                if next_id.startswith(exclude_prefixes):
                    continue
                child_node = _build_node(next_id, map_data, key, path | {node_id})
                if child_node:
                    next_nodes.append(child_node)
            if key == "parents":
                return Ancestor(
                    model_id=node_id, parents=cast(list[Ancestor], next_nodes)
                )
            return Descendant(
                model_id=node_id, children=cast(list[Descendant], next_nodes)
            )

        if direction in ("both", "parents"):
            for item_id in parent_map.get(model_id, []):
                if recursive and item_id.startswith(exclude_prefixes):
                    continue

                if recursive:
                    p_node = _build_node(item_id, parent_map, "parents", {model_id})
                    if p_node:
                        parents.append(cast(Ancestor, p_node))
                else:
                    parents.append(Ancestor(model_id=item_id))

        if direction in ("both", "children"):
            for item_id in child_map.get(model_id, []):
                if recursive and item_id.startswith(exclude_prefixes):
                    continue

                if recursive:
                    c_node = _build_node(item_id, child_map, "children", {model_id})
                    if c_node:
                        children.append(cast(Descendant, c_node))
                else:
                    children.append(Descendant(model_id=item_id))
        return cls(
            model_id=model_id,
            parents=parents,
            children=children,
        )


def get_uid_from_name(manifest: Manifest, model_id: str) -> str:
    """
    Given a dbt manifest mapping and a model name, return the unique_id
    corresponding to that model name, or None if not found.
    """
    # using the parent and child map so it include sources/exposures
    if model_id in manifest.child_map or model_id in manifest.parent_map:
        return model_id
    # fallback: look through eveything for the identifier
    for uid, node in manifest.nodes.items():
        if node.name == model_id:
            return uid
    for uid, source in manifest.sources.items():
        if source.identifier == model_id:
            return uid
    for uid, exposure in manifest.exposures.items():
        if exposure.name == model_id:
            return uid
    raise ValueError(f"Model name '{model_id}' not found in manifest.")
