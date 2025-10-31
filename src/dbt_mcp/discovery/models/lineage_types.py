from __future__ import annotations
from typing import Literal

from pydantic import BaseModel, Field
from typing import Any


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
        manifest: dict[str, Any],
        model_id: str,
        recursive: bool = False,
        direction: Literal["parents", "children", "both"] = "both",
        exclude_prefixes: tuple[str, ...] = ("test.", "unit_test."),
    ) -> ModelLineage:
        """
        Build a ModelLineage instance from a dbt manifest mapping.

        - manifest: dict containing at least 'parent_map' and/or 'child_map'
        - model_id: the model id to start from
        - recursive: whether to traverse recursively
        - direction: one of 'parents', 'children', or 'both'
        - exclude_prefixes: tuple of prefixes to exclude from descendants, defaults to ("test.", "unit_test.")
            Descendants only. Give () to include all.

        The returned ModelLineage contains lists of Ancestor and/or Descendant
        objects. For compatibility with the previous implementation, recursive
        traversal returns a flat list of Ancestor/Descendant nodes (no nested
        parents/children relationships are constructed).
        """
        parent_map = manifest.get("parent_map", {})
        child_map = manifest.get("child_map", {})

        parents: list[Ancestor] = []
        children: list[Descendant] = []

        if direction in ("both", "parents"):
            if not recursive:
                # direct parents only
                for pid in parent_map.get(model_id, []):
                    parents.append(Ancestor.model_validate({"model_id": pid}))
            else:
                # Build nested ancestor trees. We prevent cycles using path tracking.
                def _build_ancestor(node_id: str, path: set[str]) -> Ancestor:
                    if node_id in path:
                        # cycle detected, return node without parents
                        return Ancestor.model_validate({"model_id": node_id})
                    new_path = set(path)
                    new_path.add(node_id)
                    parents = [
                        _build_ancestor(pid, new_path)
                        for pid in parent_map.get(node_id, [])
                    ]
                    return Ancestor.model_validate(
                        {"model_id": node_id, "parents": parents}
                    )

                for pid in parent_map.get(model_id, []):
                    parents.append(_build_ancestor(pid, {model_id}))

        if direction in ("both", "children"):
            if not recursive:
                children = [
                    Descendant.model_validate({"model_id": cid})
                    for cid in child_map.get(model_id, [])
                ]
            else:
                # Build nested descendant trees. Prevent cycles using path tracking.
                def _build_descendant(node_id: str, path: set[str]) -> Descendant:
                    if node_id in path:
                        return Descendant.model_validate({"model_id": node_id})
                    new_path = set(path)
                    new_path.add(node_id)
                    # exclude children with specified prefixes
                    new_children = [
                        cid
                        for cid in child_map.get(node_id, [])
                        if not cid.startswith(exclude_prefixes)
                    ]

                    children = [
                        _build_descendant(cid, new_path) for cid in new_children
                    ]
                    return Descendant.model_validate(
                        {"model_id": node_id, "children": children},
                        context={"exclude_prefixes": exclude_prefixes},
                    )

                for cid in [
                    cid
                    for cid in child_map.get(model_id, [])
                    if not cid.startswith(exclude_prefixes)
                ]:
                    children.append(_build_descendant(cid, {model_id}))
        return cls(
            model_id=model_id,
            parents=parents,
            children=children,
        )
