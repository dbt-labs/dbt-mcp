import re
import subprocess
from enum import Enum

from dbt_mcp.errors import BinaryExecutionError

_VERSION_RE = re.compile(r"\b(\d+\.\d+\.\d+\S*)\b")


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
            [file_path, "--help"],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
        help_output = result.stdout
    except Exception as e:
        raise BinaryExecutionError(f"Cannot execute binary {file_path}: {e}")

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


def get_dbt_version(dbt_path: str, binary_type: BinaryType) -> str | None:
    """Return the installed dbt version string (e.g. ``'1.8.4'``), or ``None`` if unparseable.

    Runs ``dbt --version`` and parses the output according to the binary type:

    - ``DBT_CORE``: looks for ``installed: X.Y.Z`` in the output
    - ``DBT_CLOUD_CLI``: looks for ``dbt Cloud CLI - X.Y.Z``
    - ``FUSION``: falls back to the first semver found in output
    """
    try:
        result = subprocess.run(
            [dbt_path, "--version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        output = result.stdout or result.stderr
    except Exception:
        return None

    if not output:
        return None

    if binary_type == BinaryType.DBT_CORE:
        installed_match = re.search(r"installed:\s*(\d+\.\d+\.\d+\S*)", output)
        if installed_match:
            return installed_match.group(1)
    elif binary_type == BinaryType.DBT_CLOUD_CLI:
        cloud_match = re.search(r"dbt Cloud CLI - (\d+\.\d+\.\d+\S*)", output)
        if cloud_match:
            return cloud_match.group(1)

    # Fallback: find first semver anywhere in the output (covers Fusion and unknown formats)
    match = _VERSION_RE.search(output)
    return match.group(1) if match else None


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
