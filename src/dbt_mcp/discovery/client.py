import textwrap
from typing import Literal, TypedDict

import requests

from dbt_mcp.config.config_providers import ConfigProvider, DiscoveryConfig
from dbt_mcp.errors import GraphQLError, InvalidParameterError
from dbt_mcp.gql.errors import raise_gql_error

PAGE_SIZE = 100
MAX_NODE_QUERY_LIMIT = 1000
LINEAGE_LIMIT = 50  # Max nodes per direction (ancestors/descendants)


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


    GET_LINEAGE = textwrap.dedent("""
        query GetLineage($environmentId: BigInt!, $filter: LineageFilter) {
            environment(id: $environmentId) {
                applied {
                    lineage(filter: $filter) {
                        ... on ModelLineageNode {
                            uniqueId
                            name
                            resourceType
                            filePath
                            matchesMethod
                            tags
                            projectId
                            fqn
                            database
                            schema
                            alias
                            materializationType
                            lastRunStatus
                        }
                        ... on SourceLineageNode {
                            uniqueId
                            name
                            resourceType
                            filePath
                            matchesMethod
                            tags
                            projectId
                            fqn
                            database
                            schema
                            sourceName
                            lastRunStatus
                        }
                        ... on SeedLineageNode {
                            uniqueId
                            name
                            resourceType
                            filePath
                            matchesMethod
                            tags
                            projectId
                            fqn
                            database
                            schema
                            alias
                            lastRunStatus
                        }
                        ... on SnapshotLineageNode {
                            uniqueId
                            name
                            resourceType
                            filePath
                            matchesMethod
                            tags
                            projectId
                            fqn
                            database
                            schema
                            alias
                            lastRunStatus
                        }
                        ... on ExposureLineageNode {
                            uniqueId
                            name
                            resourceType
                            filePath
                            matchesMethod
                            tags
                            projectId
                            fqn
                        }
                        ... on MetricLineageNode {
                            uniqueId
                            name
                            resourceType
                            filePath
                            matchesMethod
                            tags
                            projectId
                            fqn
                        }
                        ... on SemanticModelLineageNode {
                            uniqueId
                            name
                            resourceType
                            filePath
                            matchesMethod
                            tags
                            projectId
                            fqn
                            publicParentIds
                        }
                        ... on SavedQueryLineageNode {
                            uniqueId
                            name
                            resourceType
                            filePath
                            matchesMethod
                            tags
                            projectId
                            fqn
                        }
                        ... on MacroLineageNode {
                            uniqueId
                            name
                            resourceType
                            filePath
                            matchesMethod
                            tags
                            projectId
                        }
                        ... on TestLineageNode {
                            uniqueId
                            name
                            resourceType
                            filePath
                            matchesMethod
                            tags
                            projectId
                            fqn
                            lastRunStatus
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
        """Parse GraphQL response and extract model nodes.

        Args:
            result: Raw GraphQL query response

        Returns:
            List of model node dictionaries

        Raises:
            GraphQLError: If the query contains errors
        """
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
        """Build GraphQL filter for model queries.

        Args:
            model_name: Model identifier/name to filter by
            unique_id: Model unique ID to filter by

        Returns:
            GraphQL filter dictionary

        Raises:
            InvalidParameterError: If neither parameter is provided
        """
        if unique_id is not None:
            return {"uniqueIds": [unique_id]}
        elif model_name is not None:
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


class ExposuresFetcher:
    def __init__(self, api_client: MetadataAPIClient):
        self.api_client = api_client

    async def get_environment_id(self) -> int:
        config = await self.api_client.config_provider.get_config()
        return config.environment_id

    def _parse_response_to_json(self, result: dict) -> list[dict]:
        """Parse GraphQL response and extract exposure nodes.

        Args:
            result: Raw GraphQL query response

        Returns:
            List of exposure node dictionaries

        Raises:
            GraphQLError: If the query contains errors
        """
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
        """Build GraphQL filter for exposure queries.

        Args:
            exposure_name: Not supported - raises error if provided
            unique_ids: List of exposure unique IDs to filter by

        Returns:
            GraphQL filter dictionary

        Raises:
            InvalidParameterError: If exposure_name is used or unique_ids is not provided
        """
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
        """Parse GraphQL response and extract source nodes.

        Args:
            result: Raw GraphQL query response

        Returns:
            List of source node dictionaries

        Raises:
            GraphQLError: If the query contains errors
        """
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
        """Build GraphQL filter for source queries.

        Args:
            source_name: Source identifier/name to filter by
            unique_id: Source unique ID to filter by

        Returns:
            GraphQL filter dictionary

        Raises:
            InvalidParameterError: If neither parameter is provided
        """
        if unique_id is not None:
            return {"uniqueIds": [unique_id]}
        elif source_name is not None:
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


class LineageDirection:
    ANCESTORS = "ancestors"
    DESCENDANTS = "descendants"
    BOTH = "both"


VALID_DIRECTIONS = (LineageDirection.ANCESTORS, LineageDirection.DESCENDANTS, LineageDirection.BOTH)

VALID_RESOURCE_TYPES = {
    "Model",
    "Source",
    "Seed",
    "Snapshot",
    "Exposure",
    "Metric",
    "SemanticModel",
    "SavedQuery",
    "Macro",
    "Test",
}


class LineageFetcher:
    """Fetcher for lineage data using the Discovery API's lineage query."""

    def __init__(self, api_client: MetadataAPIClient):
        self.api_client = api_client

    async def get_environment_id(self) -> int:
        config = await self.api_client.config_provider.get_config()
        return config.environment_id

    async def search_models_by_name(self, name: str) -> list[dict]:
        """Search for models by name/identifier.

        Returns list of matches with uniqueId, name, and resourceType.
        """
        variables = {
            "environmentId": await self.get_environment_id(),
            "modelsFilter": {"identifier": name},
            "first": PAGE_SIZE,
        }
        result = await self.api_client.execute_query(
            GraphQLQueries.GET_MODELS, variables
        )
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["models"]["edges"]
        if not edges:
            return []
        return [
            {
                "uniqueId": edge["node"]["uniqueId"],
                "name": edge["node"]["name"],
                "resourceType": "Model",
            }
            for edge in edges
        ]

    async def search_sources_by_name(self, name: str) -> list[dict]:
        """Search for sources by name/identifier.

        Returns list of matches with uniqueId, name, and resourceType.
        """
        variables = {
            "environmentId": await self.get_environment_id(),
            "sourcesFilter": {"identifier": name},
            "first": PAGE_SIZE,
        }
        result = await self.api_client.execute_query(
            GraphQLQueries.GET_SOURCES, variables
        )
        raise_gql_error(result)
        edges = result["data"]["environment"]["applied"]["sources"]["edges"]
        if not edges:
            return []
        return [
            {
                "uniqueId": edge["node"]["uniqueId"],
                "name": edge["node"]["name"],
                "resourceType": "Source",
            }
            for edge in edges
        ]

    async def search_all_resources(self, name: str) -> list[dict]:
        """Search for resources by name across all supported types.

        Uses pragmatic resolution: models first (most common), then sources.
        Returns all matches found.
        """
        matches: list[dict] = []

        # Search models first (fast path for ~90% of cases)
        model_matches = await self.search_models_by_name(name)
        matches.extend(model_matches)

        # Search sources
        source_matches = await self.search_sources_by_name(name)
        matches.extend(source_matches)

        # Note: Additional resource types (seeds, snapshots, etc.) could be added here
        # For now, models and sources cover the most common use cases

        return matches

    def _build_selector(self, unique_id: str, direction: str) -> str:
        """Build dbt selector syntax based on direction.

        - ancestors: +uniqueId (upstream)
        - descendants: uniqueId+ (downstream)
        - both: +uniqueId+ (both directions)
        """
        if direction == LineageDirection.ANCESTORS:
            return f"+{unique_id}"
        elif direction == LineageDirection.DESCENDANTS:
            return f"{unique_id}+"
        else:  # both
            return f"+{unique_id}+"

    async def fetch_lineage(
        self,
        unique_id: str,
        direction: str = LineageDirection.BOTH,
        types: list[str] | None = None,
    ) -> dict:
        """Fetch lineage for a resource.

        Args:
            unique_id: The dbt unique ID of the resource
            direction: One of 'ancestors', 'descendants', or 'both'
            types: Optional list of resource types to filter results

        Returns:
            Dict with 'target', 'ancestors', and/or 'descendants' keys
        """
        # For "both" direction, make two separate API calls to correctly
        # categorize ancestors and descendants (BUG-001 fix)
        if direction == LineageDirection.BOTH:
            ancestors_result = await self._fetch_lineage_single_direction(
                unique_id, LineageDirection.ANCESTORS, types
            )
            descendants_result = await self._fetch_lineage_single_direction(
                unique_id, LineageDirection.DESCENDANTS, types
            )
            # Merge results - use target from either (they should be the same)
            target = (
                ancestors_result.get("target")
                if ancestors_result.get("target") is not None
                else descendants_result.get("target")
            )

            # Merge pagination from both directions
            ancestors_pagination = ancestors_result.get("pagination", {})
            descendants_pagination = descendants_result.get("pagination", {})

            return {
                "target": target,
                "ancestors": ancestors_result.get("ancestors", []),
                "descendants": descendants_result.get("descendants", []),
                "pagination": {
                    "limit": LINEAGE_LIMIT,
                    "ancestors_total": ancestors_pagination.get("ancestors_total", 0),
                    "ancestors_truncated": ancestors_pagination.get(
                        "ancestors_truncated", False
                    ),
                    "descendants_total": descendants_pagination.get(
                        "descendants_total", 0
                    ),
                    "descendants_truncated": descendants_pagination.get(
                        "descendants_truncated", False
                    ),
                },
            }
        else:
            return await self._fetch_lineage_single_direction(
                unique_id, direction, types
            )

    async def _fetch_lineage_single_direction(
        self,
        unique_id: str,
        direction: str,
        types: list[str] | None = None,
    ) -> dict:
        """Fetch lineage for a single direction (ancestors or descendants).

        Internal method used by fetch_lineage.
        """
        selector = self._build_selector(unique_id, direction)

        lineage_filter: dict = {"uniqueIds": [selector]}
        if types is not None:
            lineage_filter["types"] = types

        variables = {
            "environmentId": await self.get_environment_id(),
            "filter": lineage_filter,
        }

        result = await self.api_client.execute_query(
            GraphQLQueries.GET_LINEAGE, variables
        )
        raise_gql_error(result)

        lineage_nodes = result["data"]["environment"]["applied"]["lineage"]
        if not lineage_nodes:
            return {"target": None, "ancestors": [], "descendants": []}

        return self._transform_lineage_response(lineage_nodes, direction)

    def _transform_lineage_response(
        self, nodes: list[dict], direction: str
    ) -> dict:
        """Transform raw lineage response into structured output.

        Separates target node (matchesMethod=true) from lineage nodes.
        Applies LINEAGE_LIMIT and includes pagination metadata.
        This method handles single-direction queries only (ancestors OR descendants).
        The "both" direction is handled by fetch_lineage via two separate calls.
        """
        target: dict | None = None
        lineage_nodes: list[dict] = []

        for node in nodes:
            if node.get("matchesMethod"):
                target = node
            else:
                lineage_nodes.append(node)

        # Apply limit and track truncation
        total = len(lineage_nodes)
        truncated = total > LINEAGE_LIMIT
        lineage_nodes = lineage_nodes[:LINEAGE_LIMIT]

        # Build response based on direction
        response: dict = {"target": target}

        if direction == LineageDirection.ANCESTORS:
            response["ancestors"] = lineage_nodes
            response["pagination"] = {
                "limit": LINEAGE_LIMIT,
                "ancestors_total": total,
                "ancestors_truncated": truncated,
            }
        elif direction == LineageDirection.DESCENDANTS:
            response["descendants"] = lineage_nodes
            response["pagination"] = {
                "limit": LINEAGE_LIMIT,
                "descendants_total": total,
                "descendants_truncated": truncated,
            }

        return response
