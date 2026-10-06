from enum import Enum


class AccessPolicy(Enum):
    """Additional policies implemented by the host, alongside target annotations."""

    LOCAL = "local"
