"""Shared field definitions."""

from pydantic import Field


_UNIQUE_ID_DESCRIPTION = (
    "Fully-qualified unique ID of the resource. "
    "This will follow the format `<resource_type>.<package_name>.<resource_name>` "
    "(e.g. `model.analytics.stg_orders`)."
)

UNIQUE_ID_FIELD = Field(
    default=None,
    description=_UNIQUE_ID_DESCRIPTION
    + " Strongly preferred over the `name` parameter for deterministic lookups.",
)

UNIQUE_ID_REQUIRED_FIELD = Field(
    description=_UNIQUE_ID_DESCRIPTION,
)

NAME_FIELD = Field(
    default=None,
    description="The name of the resource. "
    "This is not required if `unique_id` is provided. "
    "Only use name when `unique_id` is unknown.",
)

DEPTH_FIELD = Field(
    default=5,
    description="The depth of the lineage graph to return. "
    "Controls how many levels to traverse from the target node."
    "A depth of 1 returns only direct parents/children."
    "A depth of 0 returns the entire lineage graph.",
)

TYPES_FIELD = Field(
    default=None,
    description="List of resource types to include in lineage results. "
    "If not provided, includes all types. "
    "Valid types: Model, Source, Seed, Snapshot, Exposure, Metric, SemanticModel, SavedQuery, Test.",
)
