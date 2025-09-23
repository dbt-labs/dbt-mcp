import subprocess
from enum import Enum


class BinaryType(Enum):
    DBT_CORE = "dbt_core"
    FUSION = "fusion"
    DBT_CLOUD_CLI = "dbt_cloud_cli"


def detect_binary_type(file_path: str) -> BinaryType:
    """
    Detect the type of dbt binary (dbt Core, Fusion, or dbt Cloud CLI) by running --help.

    Args:
        file_path: Path to the dbt executable

    Returns:
        BinaryType: The detected binary type

    Raises:
        Exception: If the binary cannot be executed or accessed
    """
    try:
        result = subprocess.run(
            [file_path, "--help"], capture_output=True, text=True, timeout=10
        )
        help_output = result.stdout
    except Exception as e:
        raise Exception(f"Cannot execute binary {file_path}: {e}")

    if not help_output:
        # Default to dbt Core if no output
        return BinaryType.DBT_CORE

    first_line = help_output.split("\n")[0] if help_output else ""

    # Check for dbt-fusion
    if "dbt-fusion" in first_line:
        return BinaryType.FUSION

    # Check for dbt Core
    if "Usage: dbt [OPTIONS] COMMAND [ARGS]..." in first_line:
        return BinaryType.DBT_CORE

    # Check for dbt Cloud CLI
    if "The dbt Cloud CLI" in first_line:
        return BinaryType.DBT_CLOUD_CLI

    # Default to dbt Core - We could move to Fusion in the future
    return BinaryType.DBT_CORE


def get_color_disable_flag(binary_type: BinaryType) -> str:
    """
    Get the appropriate color disable flag for the given binary type.

    Args:
        binary_type: The type of dbt binary

    Returns:
        str: The color disable flag to use
    """
    if binary_type == BinaryType.DBT_CLOUD_CLI:
        return "--no-color"
    else:  # DBT_CORE or FUSION
        return "--no-use-colors"
