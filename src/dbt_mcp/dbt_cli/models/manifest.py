#!/usr/bin/env python3
# This is a SUPER simplified version of the dbt manifest.json structure,
# only including the fields we need

from collections.abc import Iterable

from pydantic import BaseModel, Field

# unique_id shape: `<resource_type>.<package>.<...>`
UNIQUE_ID_RESOURCE_TYPES: frozenset[str] = frozenset(
    {
        "analysis",
        "exposure",
        "function",
        "metric",
        "model",
        "saved_query",
        "seed",
        "semantic_model",
        "snapshot",
        "source",
        "test",
        "unit_test",
    }
)

# Selected by dbt's default FQN method.
_FQN_RESOURCE_TYPES: frozenset[str] = frozenset(
    {
        "analysis",
        "function",
        "model",
        "seed",
        "snapshot",
        "test",
    }
)

_METHOD_BY_RESOURCE_TYPE: dict[str, str] = {
    "exposure": "exposure",
    "metric": "metric",
    "saved_query": "saved_query",
    "semantic_model": "semantic_model",
    "unit_test": "unit_test",
}


def looks_like_unique_id(node_id: str) -> bool:
    """Return whether node_id has the shape of a dbt unique_id.

    A unique_id is `<resource_type>.<package>.<...>` for a known resource type.
    Method selectors such as `source:raw.payments` do not match.
    """
    parts = node_id.split(".")
    if len(parts) < 3 or parts[0] not in UNIQUE_ID_RESOURCE_TYPES:
        return False
    return all(parts[:3])


class Node(BaseModel):
    name: str = ""
    resource_type: str = ""
    package_name: str = ""
    fqn: list[str] = Field(default_factory=list)


class Source(BaseModel):
    identifier: str = ""
    name: str = ""
    source_name: str = ""
    package_name: str = ""


class Exposure(BaseModel):
    name: str = ""
    package_name: str = ""


class NamedNode(BaseModel):
    """A metric, semantic model, saved query, or unit test."""

    name: str = ""
    package_name: str = ""


class Manifest(BaseModel):
    parent_map: dict[str, list[str]] = Field(default_factory=dict)
    child_map: dict[str, list[str]] = Field(default_factory=dict)
    nodes: dict[str, Node] = Field(default_factory=dict)
    sources: dict[str, Source] = Field(default_factory=dict)
    exposures: dict[str, Exposure] = Field(default_factory=dict)
    metrics: dict[str, NamedNode] = Field(default_factory=dict)
    semantic_models: dict[str, NamedNode] = Field(default_factory=dict)
    saved_queries: dict[str, NamedNode] = Field(default_factory=dict)
    unit_tests: dict[str, NamedNode] = Field(default_factory=dict)

    def selector_for_unique_id(self, unique_id: str) -> str | None:
        """Return a `dbt list --select` selector for unique_id, or None if unknown.

        The default dbt selector is the FQN method. It matches a node's leaf
        name, or a prefix of `fqn` (package, then each subdirectory, then the
        name). `package.name` does not match a node that lives in a
        subdirectory: the second selector part is compared to that directory,
        not to the name. When the leaf name is shared with another non-source
        node, the selector is the full dotted fqn (`'.'.join(fqn)`), which is
        the most precise selector that method accepts.

        Sources, exposures, metrics, semantic models, saved queries, and unit
        tests use their method selectors. The value is package-qualified when
        the same name exists in more than one package.
        """
        resource_type = unique_id.split(".", 1)[0]
        if resource_type == "source":
            source = self.sources.get(unique_id)
            if source is None:
                return None
            return _source_selector(source, peers=self.sources.values())
        if resource_type in _METHOD_BY_RESOURCE_TYPE:
            named = self._method_node(unique_id=unique_id, resource_type=resource_type)
            if named is None:
                return None
            return _method_selector(
                method=_METHOD_BY_RESOURCE_TYPE[resource_type],
                name=named.name,
                package_name=named.package_name,
                peer_names=self._method_peer_names(resource_type),
            )
        if resource_type in _FQN_RESOURCE_TYPES:
            node = self.nodes.get(unique_id)
            if node is None:
                return None
            return _fqn_selector(
                name=node.name,
                package_name=node.package_name,
                fqn=node.fqn,
                leaf_names=self._fqn_leaf_names(),
            )
        return None

    def _method_node(self, *, unique_id: str, resource_type: str) -> NamedNode | None:
        if resource_type == "exposure":
            exposure = self.exposures.get(unique_id)
            if exposure is None or not exposure.name:
                return None
            return NamedNode(name=exposure.name, package_name=exposure.package_name)
        collection = _method_collection(self, resource_type)
        named = collection.get(unique_id)
        if named is not None and named.name:
            return named
        node = self.nodes.get(unique_id)
        if node is None or not node.name:
            return None
        return NamedNode(name=node.name, package_name=node.package_name)

    def _method_peer_names(self, resource_type: str) -> list[str]:
        names: list[str] = []
        seen: set[str] = set()

        def add(*, unique_id: str, name: str) -> None:
            if not name or unique_id in seen:
                return
            seen.add(unique_id)
            names.append(name)

        if resource_type == "exposure":
            for unique_id, exposure in self.exposures.items():
                add(unique_id=unique_id, name=exposure.name)
            return names

        for unique_id, named in _method_collection(self, resource_type).items():
            add(unique_id=unique_id, name=named.name)
        for unique_id, node in self.nodes.items():
            node_type = node.resource_type or unique_id.split(".", 1)[0]
            if node_type == resource_type:
                add(unique_id=unique_id, name=node.name)
        return names

    def _fqn_leaf_names(self) -> list[str]:
        """Leaf names the default FQN selector would consider.

        That method searches every non-source resource, so a model whose name
        matches an exposure (or another package's model) is ambiguous.
        """
        names: list[str] = []
        seen: set[str] = set()

        def add(*, unique_id: str, name: str) -> None:
            if not name or unique_id in seen:
                return
            seen.add(unique_id)
            names.append(name)

        for unique_id, node in self.nodes.items():
            add(unique_id=unique_id, name=node.name)
        for unique_id, exposure in self.exposures.items():
            add(unique_id=unique_id, name=exposure.name)
        for unique_id, metric in self.metrics.items():
            add(unique_id=unique_id, name=metric.name)
        for unique_id, semantic_model in self.semantic_models.items():
            add(unique_id=unique_id, name=semantic_model.name)
        for unique_id, saved_query in self.saved_queries.items():
            add(unique_id=unique_id, name=saved_query.name)
        for unique_id, unit_test in self.unit_tests.items():
            add(unique_id=unique_id, name=unit_test.name)
        return names


def _method_collection(manifest: Manifest, resource_type: str) -> dict[str, NamedNode]:
    collections = {
        "metric": manifest.metrics,
        "semantic_model": manifest.semantic_models,
        "saved_query": manifest.saved_queries,
        "unit_test": manifest.unit_tests,
    }
    return collections[resource_type]


def _name_is_shared(name: str, peer_names: Iterable[str]) -> bool:
    matches = 0
    for peer_name in peer_names:
        if peer_name != name:
            continue
        matches += 1
        if matches > 1:
            return True
    return False


def _fqn_selector(
    *,
    name: str,
    package_name: str,
    fqn: list[str],
    leaf_names: Iterable[str],
) -> str | None:
    if name and not _name_is_shared(name, leaf_names):
        return name
    if fqn:
        return ".".join(fqn)
    if package_name and name:
        return f"{package_name}.{name}"
    return name or None


def _method_selector(
    *,
    method: str,
    name: str,
    package_name: str,
    peer_names: Iterable[str],
) -> str:
    value = name
    if package_name and _name_is_shared(name, peer_names):
        value = f"{package_name}.{name}"
    return f"{method}:{value}"


def _source_selector(source: Source, *, peers: Iterable[Source]) -> str | None:
    table_name = source.name or source.identifier
    if not source.source_name or not table_name:
        return None
    value = f"{source.source_name}.{table_name}"
    if source.package_name and _source_table_is_shared(source, table_name, peers=peers):
        value = f"{source.package_name}.{value}"
    return f"source:{value}"


def _source_table_is_shared(
    source: Source,
    table_name: str,
    *,
    peers: Iterable[Source],
) -> bool:
    matches = 0
    for peer in peers:
        peer_table = peer.name or peer.identifier
        if peer.source_name != source.source_name or peer_table != table_name:
            continue
        matches += 1
        if matches > 1:
            return True
    return False
