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
