import json
import logging
from importlib.metadata import version

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

MANIFEST_PATH = "manifest.json"

# Get package version to sync MCPB manifest version
package_version = version("dbt-mcp")

# Update manifest.json
with open(MANIFEST_PATH, "r+") as f:
    data = json.load(f)
    data["version"] = package_version
    f.seek(0)
    json.dump(data, f, indent=4)
    f.truncate()

logger.info(f"Wrote MCPB {MANIFEST_PATH} version: {package_version}")
