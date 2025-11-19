import logging
import textwrap
from typing import Literal, TypedDict

import requests

from dbt_mcp.config.config_providers import ConfigProvider, DiscoveryConfig
from dbt_mcp.errors import GraphQLError, InvalidParameterError
from dbt_mcp.gql.errors import raise_gql_error

logger = logging.getLogger(__name__)

PAGE_SIZE = 100
MAX_NODE_QUERY_LIMIT = 1000


class GraphQLQueries:
    GET_MODELS = textwrap.dedent("""
        query GetModels(
            $environmentId: BigInt!,
            $modelsFilter: ModelAppliedFilter,
            $after: String,
            $first: Int,
            $sort: AppliedModelSort
        ) {
            environment(id: $environmentId) {
                applied {
                    models(filter: $modelsFilter, after: $after, first: $first, sort: $sort) {
                        pageInfo {
                            endCursor
                        }
                        edges {
                            node {
                                name
                                uniqueId
                                description
                            }
                        }
                    }
                }
            }
        }
    """)

    GET_MODEL_HEALTH = textwrap.dedent("""
        query GetModelDetails(
            $environmentId: BigInt!,
            $modelsFilter: ModelAppliedFilter
            $first: Int,
        ) {
            environment(id: $environmentId) {
                applied {
                    models(filter: $modelsFilter, first: $first) {
                        edges {
                            node {
                                name
                                uniqueId
                                executionInfo {
                                    lastRunGeneratedAt
                                    lastRunStatus
                                    executeCompletedAt
                                    executeStartedAt
                                }
                                tests {
                                    name
                                    description
                                    columnName
                                    testType
                                    executionInfo {
                                        lastRunGeneratedAt
                                        lastRunStatus
                                        executeCompletedAt
                                        executeStartedAt
                                    }
                                }
                                ancestors(types: [Model, Source, Seed, Snapshot]) {
                                  ... on ModelAppliedStateNestedNode {
                                    name
                                    uniqueId
                                    resourceType
                                    materializedType
                                    modelexecutionInfo: executionInfo {
                                      lastRunStatus
                                      executeCompletedAt
                                      }
                                  }
                                  ... on SnapshotAppliedStateNestedNode {
                                    name
                                    uniqueId
                                    resourceType
                                    snapshotExecutionInfo: executionInfo {
                                      lastRunStatus
                                      executeCompletedAt
                                    }
                                  }
                                  ... on SeedAppliedStateNestedNode {
                                    name
                                    uniqueId
                                    resourceType
                                    seedExecutionInfo: executionInfo {
                                      lastRunStatus
                                      executeCompletedAt
                                    }
                                  }
                                  ... on SourceAppliedStateNestedNode {
                                    sourceName
                                    name
                                    resourceType
                                    freshness {
                                      maxLoadedAt
                                      maxLoadedAtTimeAgoInS
                                      freshnessStatus
                                    }
                                  }
                              }
                            }
                        }
                    }
                }
            }
        }
    """)

    GET_MODEL_DETAILS = textwrap.dedent("""
        query GetModelDetails(
            $environmentId: BigInt!,
            $modelsFilter: ModelAppliedFilter
            $first: Int,
        ) {
            environment(id: $environmentId) {
                applied {
                    models(filter: $modelsFilter, first: $first) {
                        edges {
                            node {
                                name
                                uniqueId
                                compiledCode
                                description
                                database
                                schema
                                alias
                                catalog {
                                    columns {
                                        description
                                        name
                                        type
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    """)

    COMMON_FIELDS_PARENTS_CHILDREN = textwrap.dedent("""
        {
        ... on ExposureAppliedStateNestedNode {
            resourceType
            name
            description
        }
        ... on ExternalModelNode {
            resourceType
            description
            name
        }
        ... on MacroDefinitionNestedNode {
            resourceType
            name
            description
        }
        ... on MetricDefinitionNestedNode {
            resourceType
            name
            description
        }
        ... on ModelAppliedStateNestedNode {
            resourceType
            name
            description
        }
        ... on SavedQueryDefinitionNestedNode {
            resourceType
            name
            description
        }
        ... on SeedAppliedStateNestedNode {
            resourceType
            name
            description
        }
        ... on SemanticModelDefinitionNestedNode {
            resourceType
            name
            description
        }
        ... on SnapshotAppliedStateNestedNode {
            resourceType
            name
            description
        }
        ... on SourceAppliedStateNestedNode {
            resourceType
            sourceName
            uniqueId
            name
            description
        }
        ... on TestAppliedStateNestedNode {
            resourceType
            name
            description
        }
    """)

    GET_MODEL_PARENTS = (
        textwrap.dedent("""
        query GetModelParents(
            $environmentId: BigInt!,
            $modelsFilter: ModelAppliedFilter
            $first: Int,
        ) {
            environment(id: $environmentId) {
                applied {
                    models(filter: $modelsFilter, first: $first) {
                        pageInfo {
                            endCursor
                        }
                        edges {
                            node {
                                parents
    """)
        + COMMON_FIELDS_PARENTS_CHILDREN
        + textwrap.dedent("""
                                }
                            }
                        }
                    }
                }
            }
        }
    """)
    )

    GET_MODEL_CHILDREN = (
        textwrap.dedent("""
        query GetModelChildren(
            $environmentId: BigInt!,
            $modelsFilter: ModelAppliedFilter
            $first: Int,
        ) {
            environment(id: $environmentId) {
                applied {
                    models(filter: $modelsFilter, first: $first) {
                        pageInfo {
                            endCursor
                        }
                        edges {
                            node {
                                children
    """)
        + COMMON_FIELDS_PARENTS_CHILDREN
        + textwrap.dedent("""
                                }
                            }
                        }
                    }
                }
            }
        }
    """)
    )

    GET_MODEL_ANCESTORS = (
        textwrap.dedent("""
        query GetModelLineage(
            $environmentId: BigInt!,
            $modelsFilter: ModelAppliedFilter
            $first: Int,
        ) {
            environment(id: $environmentId) {
                applied {
                    models(filter: $modelsFilter, first: $first) {
                        edges {
                            node {
                                name
                                uniqueId
                                description
                                resourceType
                                ancestors {
    """)
        + COMMON_FIELDS_PARENTS_CHILDREN
        + textwrap.dedent("""
                                }
                            }
                        }
                    }
                }
            }
        }
    """)
    )

    GET_SOURCES = textwrap.dedent("""
        query GetSources(
            $environmentId: BigInt!,
            $sourcesFilter: SourceAppliedFilter,
            $after: String,
            $first: Int
        ) {
            environment(id: $environmentId) {
                applied {
                    sources(filter: $sourcesFilter, after: $after, first: $first) {
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                        edges {
                            node {
                                name
                                uniqueId
                                identifier
                                description
                                sourceName
                                resourceType
                                database
                                schema
                                freshness {
                                    maxLoadedAt
                                    maxLoadedAtTimeAgoInS
                                    freshnessStatus
                                }
                            }
                        }
                    }
                }
            }
        }
    """)

    GET_SOURCE_DETAILS = textwrap.dedent("""
        query GetSourceDetails(
            $environmentId: BigInt!,
            $sourcesFilter: SourceAppliedFilter,
            $first: Int
        ) {
            environment(id: $environmentId) {
                applied {
                    sources(filter: $sourcesFilter, first: $first) {
                        edges {
                            node {
                                name
                                uniqueId
                                identifier
                                description
                                sourceName
                                database
                                schema
                                freshness {
                                    maxLoadedAt
                                    maxLoadedAtTimeAgoInS
                                    freshnessStatus
                                }
                                catalog {
                                    columns {
                                        name
                                        type
                                        description
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    """)

    GET_EXPOSURES = textwrap.dedent("""
        query Exposures($environmentId: BigInt!, $first: Int, $after: String) {
            environment(id: $environmentId) {
                definition {
                    exposures(first: $first, after: $after) {
                        totalCount
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                        edges {
                            node {
                                name
                                uniqueId
                                url
                                description
                            }
                        }
                    }
                }
            }
        }
    """)

    GET_EXPOSURE_DETAILS = textwrap.dedent("""
        query ExposureDetails($environmentId: BigInt!, $filter: ExposureFilter, $first: Int) {
            environment(id: $environmentId) {
                definition {
                    exposures(first: $first, filter: $filter) {
                        edges {
                            node {
                                name
                                maturity
                                label
                                ownerEmail
                                ownerName
                                uniqueId
                                url
                                meta
                                freshnessStatus
                                exposureType
                                description
                                parents {
                                    uniqueId
                                }
                            }
                        }
                    }
                }
            }
        }
    """)


class MetadataAPIClient:
    def __init__(self, config_provider: ConfigProvider[DiscoveryConfig]):
        self.config_provider = config_provider

    async def execute_query(self, query: str, variables: dict) -> dict:
        config = await self.config_provider.get_config()
        url = config.url
        headers = config.headers_provider.get_headers()
        response = requests.post(
            url=url,
            json={"query": query, "variables": variables},
            headers=headers,
        )
        return response.json()


class ModelFilter(TypedDict, total=False):
    modelingLayer: Literal["marts"] | None


class SourceFilter(TypedDict, total=False):
    sourceNames: list[str]
    uniqueIds: list[str] | None
    identifier: str


class ModelsFetcher:
    def __init__(self, api_client: MetadataAPIClient):
        self.api_client = api_client

    async def get_environment_id(self) -> int:
        config = await self.api_client.config_provider.get_config()
        return config.environment_id

    def _parse_response_to_json(self, result: dict) -> list[dict]:
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["models"]["edges"]
        parsed_edges: list[dict] = []
        if not edges:
            return parsed_edges
        if result.get("errors"):
            raise GraphQLError(f"GraphQL query failed: {result['errors']}")
        for edge in edges:
            if not isinstance(edge, dict) or "node" not in edge:
                continue
            node = edge["node"]
            if not isinstance(node, dict):
                continue
            parsed_edges.append(node)
        return parsed_edges

    def _get_model_filters(
        self, model_name: str | None = None, unique_id: str | None = None
    ) -> dict[str, list[str] | str]:
        if unique_id:
            return {"uniqueIds": [unique_id]}
        elif model_name:
            return {"identifier": model_name}
        else:
            raise InvalidParameterError(
                "Either model_name or unique_id must be provided"
            )

    async def fetch_models(self, model_filter: ModelFilter | None = None) -> list[dict]:
        has_next_page = True
        after_cursor: str = ""
        all_edges: list[dict] = []
        while has_next_page and len(all_edges) < MAX_NODE_QUERY_LIMIT:
            variables = {
                "environmentId": await self.get_environment_id(),
                "after": after_cursor,
                "first": PAGE_SIZE,
                "modelsFilter": model_filter or {},
                "sort": {"field": "queryUsageCount", "direction": "desc"},
            }

            result = await self.api_client.execute_query(
                GraphQLQueries.GET_MODELS, variables
            )
            all_edges.extend(self._parse_response_to_json(result))

            previous_after_cursor = after_cursor
            after_cursor = result["data"]["environment"]["applied"]["models"][
                "pageInfo"
            ]["endCursor"]
            if previous_after_cursor == after_cursor:
                has_next_page = False

        return all_edges

    async def fetch_model_details(
        self, model_name: str | None = None, unique_id: str | None = None
    ) -> dict:
        model_filters = self._get_model_filters(model_name, unique_id)
        variables = {
            "environmentId": await self.get_environment_id(),
            "modelsFilter": model_filters,
            "first": 1,
        }
        result = await self.api_client.execute_query(
            GraphQLQueries.GET_MODEL_DETAILS, variables
        )
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["models"]["edges"]
        if not edges:
            return {}
        return edges[0]["node"]

    async def fetch_model_parents(
        self, model_name: str | None = None, unique_id: str | None = None
    ) -> list[dict]:
        model_filters = self._get_model_filters(model_name, unique_id)
        variables = {
            "environmentId": await self.get_environment_id(),
            "modelsFilter": model_filters,
            "first": 1,
        }
        result = await self.api_client.execute_query(
            GraphQLQueries.GET_MODEL_PARENTS, variables
        )
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["models"]["edges"]
        if not edges:
            return []
        return edges[0]["node"]["parents"]

    async def fetch_model_children(
        self, model_name: str | None = None, unique_id: str | None = None
    ) -> list[dict]:
        model_filters = self._get_model_filters(model_name, unique_id)
        variables = {
            "environmentId": await self.get_environment_id(),
            "modelsFilter": model_filters,
            "first": 1,
        }
        result = await self.api_client.execute_query(
            GraphQLQueries.GET_MODEL_CHILDREN, variables
        )
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["models"]["edges"]
        if not edges:
            return []
        return edges[0]["node"]["children"]

    async def fetch_model_health(
        self, model_name: str | None = None, unique_id: str | None = None
    ) -> list[dict]:
        model_filters = self._get_model_filters(model_name, unique_id)
        variables = {
            "environmentId": await self.get_environment_id(),
            "modelsFilter": model_filters,
            "first": 1,
        }
        result = await self.api_client.execute_query(
            GraphQLQueries.GET_MODEL_HEALTH, variables
        )
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["models"]["edges"]
        if not edges:
            return []
        return edges[0]["node"]

    async def _fetch_children_batch(
        self, unique_ids: list[str]
    ) -> dict[str, list[dict]]:
        """Fetch children for multiple models in a single API call.

        This method is optimized for batch operations during lineage traversal,
        reducing API calls from O(nodes) to O(depth).

        Args:
            unique_ids: List of model unique IDs to fetch children for

        Returns:
            Dictionary mapping each parent unique_id to its list of children.
            Parents with no children will have an empty list.
        """
        if not unique_ids:
            return {}

        variables = {
            "environmentId": await self.get_environment_id(),
            "modelsFilter": {"uniqueIds": unique_ids},
            "first": len(unique_ids),
        }

        result = await self.api_client.execute_query(
            GraphQLQueries.GET_MODEL_CHILDREN, variables
        )
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["models"]["edges"]

        # Build mapping: parent_id -> children
        children_map: dict[str, list[dict]] = {}
        for edge in edges:
            node = edge["node"]
            parent_id = node.get("uniqueId")
            children = node.get("children", [])
            if parent_id:
                children_map[parent_id] = children

        # Ensure all requested IDs are in the map (even if no children)
        for uid in unique_ids:
            if uid not in children_map:
                children_map[uid] = []

        return children_map
    
    async def _fetch_all_descendants(
        self, unique_id: str, max_depth: int = 50, max_nodes: int = 1000
    ) -> tuple[list[dict], bool, int]:
        """Recursively fetch all descendants using BFS traversal.

        Args:
            unique_id: The unique ID of the starting model
            max_depth: Maximum depth (levels) to traverse (prevents deep chains)
            max_nodes: Maximum total descendant nodes to collect (prevents wide graphs)

        Returns:
            Tuple of (descendants_list, was_truncated, depth_reached):
                - descendants_list: List of all descendant nodes (limited by max_depth and max_nodes)
                - was_truncated: True if results were truncated due to hitting limits
                - depth_reached: The actual depth reached during traversal
        """
        visited: set[str] = set()
        all_descendants: list[dict] = []
        current_level = [unique_id]
        visited.add(unique_id)
        was_truncated = False
        depth_reached = 0

        for depth in range(max_depth):
            if not current_level:
                break

            depth_reached = depth

            # Stop if we've already collected enough nodes
            if len(all_descendants) >= max_nodes:
                was_truncated = True
                break

            next_level = []
            # Fetch children for all nodes at this level in a single batch API call
            children_by_parent = await self._fetch_children_batch(current_level)

            for node_id in current_level:
                children = children_by_parent.get(node_id, [])
                for child in children:
                    child_unique_id = child.get("uniqueId")
                    if child_unique_id and child_unique_id not in visited:
                        visited.add(child_unique_id)
                        all_descendants.append(child)
                        next_level.append(child_unique_id)

                        # Stop collecting if we hit the node limit
                        if len(all_descendants) >= max_nodes:
                            was_truncated = True
                            break

                # Break outer loop if we hit limit
                if len(all_descendants) >= max_nodes:
                    break

            # Check if we stopped early due to max_depth
            if next_level and depth + 1 >= max_depth:
                was_truncated = True

            current_level = next_level

        return all_descendants, was_truncated, depth_reached

    async def fetch_model_ancestors(
        self,
        model_name: str | None = None,
        unique_id: str | None = None,
    ) -> dict:
        """Fetch all ancestors (upstream dependencies) for a model.

        Args:
            model_name: The name of the model
            unique_id: The unique ID of the model

        Returns:
            Dictionary containing model info and ancestors list
        """
        model_filters = self._get_model_filters(model_name, unique_id)
        variables = {
            "environmentId": await self.get_environment_id(),
            "modelsFilter": model_filters,
            "first": 1,
        }
        result = await self.api_client.execute_query(
            GraphQLQueries.GET_MODEL_ANCESTORS, variables
        )
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["models"]["edges"]
        if not edges:
            return {}

        node = edges[0]["node"]
        return {
            "name": node.get("name"),
            "uniqueId": node.get("uniqueId"),
            "description": node.get("description"),
            "resourceType": node.get("resourceType"),
            "ancestors": node.get("ancestors", []),
        }

    async def fetch_model_descendants(
        self,
        model_name: str | None = None,
        unique_id: str | None = None,
        max_depth: int = 50,
        max_nodes: int = 1000,
    ) -> dict:
        """Fetch all descendants (downstream dependencies) for a model.

        Args:
            model_name: The name of the model
            unique_id: The unique ID of the model
            max_depth: Maximum depth (levels) to traverse in the dependency graph (default: 50)
            max_nodes: Maximum total descendant nodes to collect (default: 1000)

        Returns:
            Dictionary containing model info and descendants list
        """
        # Get model basic info and uniqueId
        model_filters = self._get_model_filters(model_name, unique_id)
        variables = {
            "environmentId": await self.get_environment_id(),
            "modelsFilter": model_filters,
            "first": 1,
        }
        result = await self.api_client.execute_query(
            GraphQLQueries.GET_MODEL_ANCESTORS, variables
        )
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["models"]["edges"]
        if not edges:
            return {}

        node = edges[0]["node"]
        model_unique_id = node.get("uniqueId")

        # Fetch all descendants using BFS traversal
        descendants = []
        warnings = []
        if model_unique_id:
            try:
                descendants, was_truncated, depth_reached = await self._fetch_all_descendants(
                    model_unique_id, max_depth, max_nodes
                )

                # Add warnings if limits were hit
                if was_truncated:
                    if len(descendants) >= max_nodes:
                        warnings.append(
                            f"Reached max_nodes limit ({max_nodes}). Results may be incomplete. "
                            f"Increase max_nodes parameter to fetch more descendants."
                        )
                    if depth_reached >= max_depth - 1:
                        warnings.append(
                            f"Reached max_depth limit ({max_depth}). Results may be incomplete. "
                            f"Increase max_depth parameter to traverse deeper."
                        )
            except Exception as e:
                # Log the error for debugging
                logger.error(
                    f"Failed to fetch descendants for {model_unique_id}: {type(e).__name__}: {str(e)}",
                    exc_info=True,
                )
                warnings.append(f"Descendants fetch failed: {type(e).__name__}: {str(e)}")

        # Return model info with descendants
        result_dict = {
            "name": node.get("name"),
            "uniqueId": node.get("uniqueId"),
            "description": node.get("description"),
            "resourceType": node.get("resourceType"),
            "descendants": descendants,
        }

        # Add warnings if any exist
        if warnings:
            result_dict["warnings"] = warnings

        return result_dict


class ExposuresFetcher:
    def __init__(self, api_client: MetadataAPIClient):
        self.api_client = api_client

    async def get_environment_id(self) -> int:
        config = await self.api_client.config_provider.get_config()
        return config.environment_id

    def _parse_response_to_json(self, result: dict) -> list[dict]:
        raise_gql_error(result)
        edges = result["data"]["environment"]["definition"]["exposures"]["edges"]
        parsed_edges: list[dict] = []
        if not edges:
            return parsed_edges
        if result.get("errors"):
            raise GraphQLError(f"GraphQL query failed: {result['errors']}")
        for edge in edges:
            if not isinstance(edge, dict) or "node" not in edge:
                continue
            node = edge["node"]
            if not isinstance(node, dict):
                continue
            parsed_edges.append(node)
        return parsed_edges

    async def fetch_exposures(self) -> list[dict]:
        has_next_page = True
        after_cursor: str | None = None
        all_edges: list[dict] = []

        while has_next_page:
            variables: dict[str, int | str] = {
                "environmentId": await self.get_environment_id(),
                "first": PAGE_SIZE,
            }
            if after_cursor:
                variables["after"] = after_cursor

            result = await self.api_client.execute_query(
                GraphQLQueries.GET_EXPOSURES, variables
            )
            new_edges = self._parse_response_to_json(result)
            all_edges.extend(new_edges)

            page_info = result["data"]["environment"]["definition"]["exposures"][
                "pageInfo"
            ]
            has_next_page = page_info.get("hasNextPage", False)
            after_cursor = page_info.get("endCursor")

        return all_edges

    def _get_exposure_filters(
        self, exposure_name: str | None = None, unique_ids: list[str] | None = None
    ) -> dict[str, list[str]]:
        if unique_ids:
            return {"uniqueIds": unique_ids}
        elif exposure_name:
            raise InvalidParameterError(
                "ExposureFilter only supports uniqueIds. Please use unique_ids parameter instead of exposure_name."
            )
        else:
            raise InvalidParameterError(
                "unique_ids must be provided for exposure filtering"
            )

    async def fetch_exposure_details(
        self, exposure_name: str | None = None, unique_ids: list[str] | None = None
    ) -> list[dict]:
        if exposure_name and not unique_ids:
            # Since ExposureFilter doesn't support filtering by name,
            # we need to fetch all exposures and find the one with matching name
            all_exposures = await self.fetch_exposures()
            for exposure in all_exposures:
                if exposure.get("name") == exposure_name:
                    return [exposure]
            return []
        elif unique_ids:
            exposure_filters = self._get_exposure_filters(unique_ids=unique_ids)
            variables = {
                "environmentId": await self.get_environment_id(),
                "filter": exposure_filters,
                "first": len(unique_ids),  # Request as many as we're filtering for
            }
            result = await self.api_client.execute_query(
                GraphQLQueries.GET_EXPOSURE_DETAILS, variables
            )
            raise_gql_error(result)
            edges = result["data"]["environment"]["definition"]["exposures"]["edges"]
            if not edges:
                return []
            return [edge["node"] for edge in edges]
        else:
            raise InvalidParameterError(
                "Either exposure_name or unique_ids must be provided"
            )


class SourcesFetcher:
    def __init__(self, api_client: MetadataAPIClient):
        self.api_client = api_client

    async def get_environment_id(self) -> int:
        config = await self.api_client.config_provider.get_config()
        return config.environment_id

    def _parse_response_to_json(self, result: dict) -> list[dict]:
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["sources"]["edges"]
        parsed_edges: list[dict] = []
        if not edges:
            return parsed_edges
        if result.get("errors"):
            raise GraphQLError(f"GraphQL query failed: {result['errors']}")
        for edge in edges:
            if not isinstance(edge, dict) or "node" not in edge:
                continue
            node = edge["node"]
            if not isinstance(node, dict):
                continue
            parsed_edges.append(node)
        return parsed_edges

    async def fetch_sources(
        self,
        source_names: list[str] | None = None,
        unique_ids: list[str] | None = None,
    ) -> list[dict]:
        source_filter: SourceFilter = {}
        if source_names is not None:
            source_filter["sourceNames"] = source_names
        if unique_ids is not None:
            source_filter["uniqueIds"] = unique_ids

        has_next_page = True
        after_cursor: str = ""
        all_edges: list[dict] = []

        while has_next_page and len(all_edges) < MAX_NODE_QUERY_LIMIT:
            variables = {
                "environmentId": await self.get_environment_id(),
                "after": after_cursor,
                "first": PAGE_SIZE,
                "sourcesFilter": source_filter,
            }

            result = await self.api_client.execute_query(
                GraphQLQueries.GET_SOURCES, variables
            )
            all_edges.extend(self._parse_response_to_json(result))

            page_info = result["data"]["environment"]["applied"]["sources"]["pageInfo"]
            has_next_page = page_info.get("hasNextPage", False)
            after_cursor = page_info.get("endCursor")

        return all_edges

    def _get_source_filters(
        self, source_name: str | None = None, unique_id: str | None = None
    ) -> dict[str, list[str] | str]:
        if unique_id:
            return {"uniqueIds": [unique_id]}
        elif source_name:
            return {"identifier": source_name}
        else:
            raise InvalidParameterError(
                "Either source_name or unique_id must be provided"
            )

    async def fetch_source_details(
        self, source_name: str | None = None, unique_id: str | None = None
    ) -> dict:
        """Fetch detailed information about a specific source including columns."""
        source_filters = self._get_source_filters(source_name, unique_id)
        variables = {
            "environmentId": await self.get_environment_id(),
            "sourcesFilter": source_filters,
            "first": 1,
        }

        result = await self.api_client.execute_query(
            GraphQLQueries.GET_SOURCE_DETAILS, variables
        )
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["sources"]["edges"]
        if not edges:
            return {}
        return edges[0]["node"]
