"""Shared Parameter Types."""

from enum import StrEnum


class LineageResourceType(StrEnum):
    """Resource types supported by lineage APIs."""

    MODEL = "Model"
    SOURCE = "Source"
    SEED = "Seed"
    SNAPSHOT = "Snapshot"
    EXPOSURE = "Exposure"
    METRIC = "Metric"
    SEMANTIC_MODEL = "SemanticModel"
    SAVED_QUERY = "SavedQuery"
    TEST = "Test"


class LineageDirection(StrEnum):
    """Which direction(s) of the lineage graph to include, relative to the target."""

    UPSTREAM = "upstream"
    DOWNSTREAM = "downstream"
    BOTH = "both"
