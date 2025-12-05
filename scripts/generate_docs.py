import argparse
import logging
import re
import sys
from pathlib import Path

from dbt_mcp.tools.toolsets import toolsets

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

README_PATH = Path(__file__).parents[1] / "README.md"
DIAGRAM_PATH = Path(__file__).parents[1] / "docs" / "diagram.d2"


def format_toolset_heading(toolset_value: str) -> str:
    """
    Format toolset enum value into a readable heading using regex.

    Examples:
        sql -> SQL
        semantic_layer -> Semantic Layer
        dbt_cli -> dbt CLI
        admin_api -> Admin API
    """
    text = toolset_value.replace("_", " ").title()

    # Uppercase common acronyms
    text = re.sub(r"\b(Sql|Api|Cli|Lsp)\b", lambda m: m.group(1).upper(), text)

    # "dbt" stylized as lowercase
    text = re.sub(r"\bDbt\b", "dbt", text)

    return text


def generate_readme_tools_section() -> str:
    """Generate the Tools section markdown from toolsets."""
    lines = ["## Tools", ""]

    for toolset, tool_names in toolsets.items():
        heading = format_toolset_heading(toolset.value)
        lines.append(f"### {heading}")

        sorted_tools = sorted([tool.value for tool in tool_names])
        for tool in sorted_tools:
            lines.append(f"- `{tool}`")

        lines.append("")  # Empty line after each section

    return "\n".join(lines)


def generate_diagram_tools_section() -> str:
    """Generate the tools section for diagram.d2 from toolsets."""
    lines = ["tools: Tools {"]

    for toolset in toolsets.keys():
        # Use enum value directly as key (e.g., "dbt_cli", "semantic_layer")
        key = toolset.value
        # Reuse same formatting as README headings
        label = format_toolset_heading(toolset.value)

        lines.append(f"  {key}: {label} {{")
        lines.append("    style.border-radius: 8")
        lines.append("  }")
        lines.append("")  # Empty line between tools

    lines.append("}")

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
    new_tools_section = generate_readme_tools_section()

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
            logger.info("✓ README.md tools section is up to date")
        else:
            logger.error("✗ README.md tools section is out of date")
        return is_up_to_date
    else:
        README_PATH.write_text(updated_content)
        status = "was already up to date" if is_up_to_date else "updated successfully"
        logger.info(f"README.md tools section {status}")
        return True


def update_d2_diagram(check_only: bool = False) -> bool:
    """
    Update the tools section in diagram.d2.

    Args:
        check_only: If True, only check if update is needed without writing.

    Returns:
        True if diagram is up to date (or was updated), False if update is needed.
    """
    if not DIAGRAM_PATH.exists():
        logger.error(f"diagram.d2 not found at {DIAGRAM_PATH}")
        return False

    diagram_content = DIAGRAM_PATH.read_text()
    new_tools_section = generate_diagram_tools_section()

    # Replace tools section (from "tools: Tools {" to closing "}")
    pattern = r"tools: Tools \{.*?\n\}"
    if not re.search(pattern, diagram_content, re.DOTALL):
        logger.error("Could not find 'tools: Tools {' section in diagram.d2")
        return False

    updated_content = re.sub(
        pattern, new_tools_section, diagram_content, flags=re.DOTALL
    )
    is_up_to_date = diagram_content == updated_content

    if check_only:
        if is_up_to_date:
            logger.info("✓ diagram.d2 tools section is up to date")
        else:
            logger.error("✗ diagram.d2 tools section is out of date")
        return is_up_to_date
    else:
        DIAGRAM_PATH.write_text(updated_content)
        status = "was already up to date" if is_up_to_date else "updated successfully"
        logger.info(f"diagram.d2 tools section {status}")
        return True


def update_all(check_only: bool = False) -> bool:
    """
    Update both README.md and diagram.d2.

    Args:
        check_only: If True, only check if updates are needed without writing.

    Returns:
        True if all docs are up to date (or were updated), False if updates needed.
    """
    readme_ok = update_readme(check_only)
    diagram_ok = update_d2_diagram(check_only)

    if check_only and not (readme_ok and diagram_ok):
        logger.error("\nRun: uv run scripts/generate_docs.py")

    return readme_ok and diagram_ok


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate or validate documentation from toolsets"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check if documentation is up to date without modifying it",
    )
    args = parser.parse_args()

    success = update_all(check_only=args.check)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
