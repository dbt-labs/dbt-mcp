import argparse
import logging
import re
import sys
from pathlib import Path

from dbt_mcp.tools.toolsets import Toolset, toolsets

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

README_PATH = Path(__file__).parents[1] / "README.md"

TOOLSET_TO_README_HEADING = {
    Toolset.DBT_CLI: "dbt CLI commands",
    Toolset.SEMANTIC_LAYER: "Semantic Layer",
    Toolset.DISCOVERY: "Discovery",
    Toolset.SQL: "SQL",
    Toolset.ADMIN_API: "Admin API",
    Toolset.DBT_CODEGEN: "Codegen",
    Toolset.DBT_LSP: "dbt LSP",
}


def generate_tools_section() -> str:
    """Generate the Tools section markdown from toolsets."""
    # Validate all toolsets have README headings defined
    missing_headings = set(toolsets.keys()) - set(TOOLSET_TO_README_HEADING.keys())
    if missing_headings:
        missing_names = [ts.name for ts in missing_headings]
        raise ValueError(
            f"Missing README headings for toolsets: {missing_names}\n"
            f"Add them to TOOLSET_TO_README_HEADING in scripts/generate_readme_tools.py"
        )

    lines = ["## Tools", ""]

    for toolset, tool_names in toolsets.items():
        heading = TOOLSET_TO_README_HEADING[toolset]
        lines.append(f"### {heading}")

        sorted_tools = sorted([tool.value for tool in tool_names])
        for tool in sorted_tools:
            lines.append(f"- `{tool}`")

        lines.append("")  # Empty line after each section

    return "\n".join(lines)


def update_readme(check_only: bool = False) -> bool:
    """
    Update the Tools section in README.md.

    Args:
        check_only: If True, only check if update is needed without writing.

    Returns:
        True if README is up to date (or was updated), False if update is needed.
    """
    if not README_PATH.exists():
        logger.error(f"README.md not found at {README_PATH}")
        return False

    readme_content = README_PATH.read_text()
    new_tools_section = generate_tools_section()

    # Replace Tools section (from "## Tools" to next "##" heading or end)
    pattern = r"(## Tools\n).*?(?=\n## |\Z)"
    if not re.search(pattern, readme_content, re.DOTALL):
        logger.error("Could not find '## Tools' section in README.md")
        return False

    updated_content = re.sub(
        pattern, new_tools_section + "\n", readme_content, flags=re.DOTALL
    )
    is_up_to_date = readme_content == updated_content

    if check_only:
        if is_up_to_date:
            logger.info("README.md tools section is up to date")
        else:
            logger.error("README.md tools section is out of date")
            logger.error("Run: uv run scripts/generate_readme_tools.py")
        return is_up_to_date

    README_PATH.write_text(updated_content)
    status = "was already up to date" if is_up_to_date else "updated successfully"
    logger.info(f"README.md tools section {status}")
    return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate or validate README tools section"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check if README is up to date without modifying it",
    )
    args = parser.parse_args()

    success = update_readme(check_only=args.check)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
